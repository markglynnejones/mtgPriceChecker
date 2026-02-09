import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import pandas as pd
import requests


SCRYFALL_COLLECTION_URL = "https://api.scryfall.com/cards/collection"


LANG_MAP = {
    "English": "en",
    "Japanese": "ja",
    "German": "de",
    "French": "fr",
    "Italian": "it",
    "Spanish": "es",
    "Portuguese": "pt",
    "Russian": "ru",
    "Korean": "ko",
    "Chinese Simplified": "zhs",
    "Chinese Traditional": "zht",
}


def normalise_lang(s: str) -> str:
    if not isinstance(s, str) or not s.strip():
        return "en"
    return LANG_MAP.get(s.strip(), "en")


def foil_kind(v: Any) -> str:
    # Moxfield export examples: NaN / "foil" / "etched"
    if not isinstance(v, str) or not v.strip():
        return "nonfoil"
    v = v.strip().lower()
    if v == "foil":
        return "foil"
    if v == "etched":
        return "etched"
    return "nonfoil"


def pick_price_eur(prices: Dict[str, Any], kind: str) -> float | None:
    # Scryfall: eur, eur_foil, eur_etched (strings or null)
    key = {"nonfoil": "eur", "foil": "eur_foil", "etched": "eur_etched"}.get(kind, "eur")
    val = prices.get(key)
    if val is None:
        # fall back to nonfoil eur if specific variant missing
        val = prices.get("eur")
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


def discord_post(webhook_url: str, content: str) -> None:
    if not webhook_url:
        return
    r = requests.post(webhook_url, json={"content": content}, timeout=30)
    r.raise_for_status()


def chunk(items: List[Dict[str, Any]], n: int) -> List[List[Dict[str, Any]]]:
    return [items[i:i+n] for i in range(0, len(items), n)]


def load_snapshot(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_snapshot(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to Moxfield export CSV")
    ap.add_argument("--snapshot", default="data/last_prices.json", help="Where to store last run prices")
    ap.add_argument("--spike_pct", type=float, default=30.0, help="Spike threshold percent (day-over-day)")
    ap.add_argument("--spike_abs", type=float, default=2.0, help="Spike threshold absolute EUR increase")
    ap.add_argument("--dip_pct", type=float, default=-25.0, help="Dip threshold percent (negative)")
    ap.add_argument("--min_price", type=float, default=1.5, help="Ignore cards below this EUR price")
    args = ap.parse_args()

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    df = pd.read_csv(args.csv)
    # Expected columns from your file:
    # Count, Name, Edition, Collector Number, Language, Foil, Proxy, etc.
    required = ["Count", "Name", "Edition", "Collector Number", "Language", "Foil"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    # Ignore proxies if present and True
    if "Proxy" in df.columns:
        df = df[df["Proxy"] != True]

    df["lang_code"] = df["Language"].apply(normalise_lang)
    df["foil_kind"] = df["Foil"].apply(foil_kind)
    df["set_code"] = df["Edition"].astype(str).str.strip().str.lower()
    df["collector"] = df["Collector Number"].astype(str).str.strip()

    # Group to unique printings/variants (so we don't query duplicates)
    grouped = (
        df.groupby(["set_code", "collector", "lang_code", "foil_kind"], dropna=False)
        .agg(total_qty=("Count", "sum"), name=("Name", "first"))
        .reset_index()
    )

    identifiers = []
    key_to_meta: Dict[str, Dict[str, Any]] = {}
    for _, row in grouped.iterrows():
        ident = {
            "set": row["set_code"],
            "collector_number": row["collector"],
            "lang": row["lang_code"],
        }
        identifiers.append({"set": ident["set"], "collector_number": ident["collector_number"], "lang": ident["lang"]})
        key = f'{ident["set"]}|{ident["collector_number"]}|{ident["lang"]}|{row["foil_kind"]}'
        key_to_meta[key] = {
            "name": row["name"],
            "set": ident["set"],
            "collector_number": ident["collector_number"],
            "lang": ident["lang"],
            "foil_kind": row["foil_kind"],
            "qty": int(row["total_qty"]),
        }

    # Load yesterday snapshot
    prev = load_snapshot(args.snapshot)

    now = datetime.now(timezone.utc).isoformat()

    current: Dict[str, Any] = {"_meta": {"generated_at": now}, "cards": {}}
    alerts: List[str] = []

    # Query Scryfall in batches of up to 75
    for batch in chunk(identifiers, 75):
        payload = {"identifiers": batch}
        r = requests.post(SCRYFALL_COLLECTION_URL, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        cards = data.get("data", [])

        # Build lookup by set|collector|lang
        by_id: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for c in cards:
            set_code = str(c.get("set", "")).lower()
            collector_number = str(c.get("collector_number", "")).strip()
            lang = str(c.get("lang", "en")).lower()
            by_id[(set_code, collector_number, lang)] = c

        for ident in batch:
            sc = ident["set"]
            cn = ident["collector_number"]
            lang = ident["lang"]
            c = by_id.get((sc, cn, lang))
            if not c:
                # Could be a mismatch (promo/variant). We skip silently but record for visibility.
                continue

            prices = c.get("prices", {}) or {}

            # We store 3 variants, and later pick based on foil_kind per row
            base_key_prefix = f"{sc}|{cn}|{lang}|"
            for kind in ("nonfoil", "foil", "etched"):
                k = base_key_prefix + kind
                meta = key_to_meta.get(k)
                if not meta:
                    continue
                eur = pick_price_eur(prices, kind)
                current["cards"][k] = {
                    **meta,
                    "scryfall_uri": c.get("scryfall_uri"),
                    "printed_name": c.get("printed_name"),
                    "eur": eur,
                }

        time.sleep(0.12)  # be polite

    prev_cards = (prev.get("cards") or {}) if isinstance(prev, dict) else {}
    curr_cards = current["cards"]

    def get_prev_eur(k: str) -> float | None:
        try:
            v = prev_cards.get(k, {}).get("eur")
            return float(v) if v is not None else None
        except Exception:
            return None

    for k, info in curr_cards.items():
        eur = info.get("eur")
        if eur is None or eur < args.min_price:
            continue

        prev_eur = get_prev_eur(k)
        if prev_eur is None or prev_eur <= 0:
            continue

        delta = eur - prev_eur
        pct = (delta / prev_eur) * 100.0

        # Spike
        if (pct >= args.spike_pct) or (delta >= args.spike_abs):
            alerts.append(
                f"📈 **PRICE SPIKE**\n"
                f"**{info['name']}** ({info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']})\n"
                f"Yesterday: €{prev_eur:.2f}\n"
                f"Today: €{eur:.2f} (**{pct:+.0f}%**, {delta:+.2f})\n"
                f"{info.get('scryfall_uri','')}"
            )

        # Dip
        if pct <= args.dip_pct:
            alerts.append(
                f"📉 **PRICE DIP**\n"
                f"**{info['name']}** ({info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']})\n"
                f"Yesterday: €{prev_eur:.2f}\n"
                f"Today: €{eur:.2f} (**{pct:+.0f}%**, {delta:+.2f})\n"
                f"{info.get('scryfall_uri','')}"
            )

    # Post alerts
    if webhook and alerts:
        header = f"🧾 MTG price watch — {datetime.now(timezone.utc).strftime('%Y-%m-%d')} (EUR via Scryfall)\nAlerts: {len(alerts)}"
        discord_post(webhook, header)
        # Discord message limit: keep chunks reasonable
        msg = ""
        for a in alerts:
            if len(msg) + len(a) + 2 > 1800:
                discord_post(webhook, msg)
                msg = a
            else:
                msg = (msg + "\n\n" + a).strip()
        if msg:
            discord_post(webhook, msg)
    elif webhook and not alerts:
        discord_post(webhook, f"🧾 MTG price watch — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\nNo alerts today.")

    # Save snapshot for tomorrow
    save_snapshot(args.snapshot, current)


if __name__ == "__main__":
    main()
