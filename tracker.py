import argparse
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import pandas as pd
import requests
import hashlib
from zoneinfo import ZoneInfo


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


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

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
    """
    Safe snapshot load:
    - missing file -> {}
    - empty file -> {}
    - invalid json -> {}
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except Exception:
        return {}


def save_snapshot(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def eur_to_gbp_rate() -> float | None:
    """
    ECB daily FX rates: returns GBP per 1 EUR.
    """
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    for node in root.iter():
        if node.attrib.get("currency") == "GBP":
            return float(node.attrib["rate"])
    return None


def should_run_now(tz_name: str, run_times_csv: str) -> bool:
    """
    If run_times_csv is provided (e.g. "07:00,19:00"), only run at those exact local times.
    """
    if not run_times_csv.strip():
        return True
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    now_hm = now_local.strftime("%H:%M")
    allowed = {t.strip() for t in run_times_csv.split(",") if t.strip()}
    return now_hm in allowed


def parse_weekday(s: str) -> int:
    """
    MON..SUN -> 0..6
    """
    days = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
    s = (s or "").strip().upper()
    return days.get(s, 6)  # default Sunday


def is_weekly_time(tz_name: str, weekly_day: str, weekly_time: str) -> bool:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    wd_target = parse_weekday(weekly_day)
    hm_target = (weekly_time or "19:00").strip()
    return now_local.weekday() == wd_target and now_local.strftime("%H:%M") == hm_target


def safe_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def write_weekly_summary_csv(
    out_path: str,
    cards: Dict[str, Any],
    rate_gbp_per_eur: float | None,
    prev_cards: Dict[str, Any],
) -> None:
    rows = []
    for k, info in cards.items():
        eur = safe_float(info.get("eur"))
        gbp = (eur * rate_gbp_per_eur) if (eur is not None and rate_gbp_per_eur is not None) else None

        prev_eur = safe_float(prev_cards.get(k, {}).get("eur"))
        prev_gbp = (prev_eur * rate_gbp_per_eur) if (prev_eur is not None and rate_gbp_per_eur is not None) else None

        delta_eur = (eur - prev_eur) if (eur is not None and prev_eur is not None) else None
        delta_gbp = (gbp - prev_gbp) if (gbp is not None and prev_gbp is not None) else None
        pct = ((delta_eur / prev_eur) * 100.0) if (delta_eur is not None and prev_eur not in (None, 0)) else None

        rows.append({
            "name": info.get("name"),
            "set": info.get("set"),
            "collector_number": info.get("collector_number"),
            "lang": info.get("lang"),
            "foil_kind": info.get("foil_kind"),
            "qty": info.get("qty"),
            "eur": eur,
            "gbp": gbp,
            "prev_eur": prev_eur,
            "prev_gbp": prev_gbp,
            "delta_eur": delta_eur,
            "delta_gbp": delta_gbp,
            "pct_change": pct,
            "scryfall_uri": info.get("scryfall_uri"),
            "cardmarket_url": info.get("cardmarket_url"),
        })

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df = pd.DataFrame(rows)
    df.sort_values(by=["name", "set", "collector_number", "foil_kind"], inplace=True, kind="mergesort")
    df.to_csv(out_path, index=False, encoding="utf-8")


def build_weekly_digest(
    curr_cards: Dict[str, Any],
    prev_cards: Dict[str, Any],
    rate_gbp_per_eur: float | None,
    min_price: float,
    top_n: int = 10,
) -> str:
    movers = []
    for k, info in curr_cards.items():
        eur = safe_float(info.get("eur"))
        if eur is None or eur < min_price:
            continue

        prev_eur = safe_float(prev_cards.get(k, {}).get("eur"))
        if prev_eur is None or prev_eur <= 0:
            continue

        delta_eur = eur - prev_eur
        pct = (delta_eur / prev_eur) * 100.0

        gbp = (eur * rate_gbp_per_eur) if (rate_gbp_per_eur is not None) else None
        prev_gbp = (prev_eur * rate_gbp_per_eur) if (rate_gbp_per_eur is not None) else None
        delta_gbp = (gbp - prev_gbp) if (gbp is not None and prev_gbp is not None) else None

        movers.append((pct, delta_eur, delta_gbp, info))

    if not movers:
        return "📊 **Weekly digest (your collection)**\nNo movers with enough price history to report yet."

    gainers = sorted(movers, key=lambda x: x[0], reverse=True)[:top_n]
    losers = sorted(movers, key=lambda x: x[0])[:top_n]

    def fmt_line(pct, delta_eur, delta_gbp, info):
        name = info["name"]
        tag = f"{info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']}"
        links = " | ".join([u for u in [info.get("scryfall_uri"), info.get("cardmarket_url")] if u])
        gbp_part = f", £{delta_gbp:+.2f}" if delta_gbp is not None else ""
        return f"- **{name}** ({tag}) — **{pct:+.0f}%** (€{delta_eur:+.2f}{gbp_part})\n  {links}"

    lines = ["📊 **Weekly digest (your collection)**", "", "**Top gainers**"]
    for pct, d_eur, d_gbp, info in gainers:
        lines.append(fmt_line(pct, d_eur, d_gbp, info))

    lines += ["", "**Top losers**"]
    for pct, d_eur, d_gbp, info in losers:
        lines.append(fmt_line(pct, d_eur, d_gbp, info))

    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to Moxfield export CSV")
    ap.add_argument("--snapshot", default="data/last_prices.json", help="Where to store last run prices")
    ap.add_argument("--spike_pct", type=float, default=30.0, help="Spike threshold percent (day-over-day)")
    ap.add_argument("--spike_abs", type=float, default=2.0, help="Spike threshold absolute EUR increase")
    ap.add_argument("--dip_pct", type=float, default=-25.0, help="Dip threshold percent (negative)")
    ap.add_argument("--min_price", type=float, default=1.5, help="Ignore cards below this EUR price")
    ap.add_argument("--tz", default="Europe/London", help="Timezone for run gating, e.g. Europe/London")
    ap.add_argument("--run-times", default="07:00,19:00", help="Comma-separated local times to run, e.g. 07:00,19:00")
    ap.add_argument("--weekly-day", default="SUN", help="Weekly summary day (MON..SUN), default SUN")
    ap.add_argument("--weekly-time", default="19:00", help="Weekly summary local time, default 19:00")
    ap.add_argument("--baseline-on-csv-change", action="store_true", help="If CSV changed, run a baseline snapshot update and skip alerts")
    args = ap.parse_args()

    prev = load_snapshot(args.snapshot)
    prev_cards = (prev.get("cards") or {}) if isinstance(prev, dict) else {}

    prev_suppress_next_no_alerts = False
    try:
        prev_suppress_next_no_alerts = bool(prev.get("_meta", {}).get("suppress_next_no_alerts"))
    except Exception:
        prev_suppress_next_no_alerts = False


    prev_was_baseline = False
    try:
        prev_was_baseline = (prev.get("_meta", {}).get("run_type") == "baseline")
    except Exception:
        prev_was_baseline = False


    csv_hash = file_sha256(args.csv)
    prev_hash = None
    try:
        prev_hash = prev.get("_meta", {}).get("csv_sha256")
    except Exception:
        prev_hash = None

    csv_changed = (prev_hash != csv_hash)

    # Gate to run times (so you can schedule the workflow hourly).
    if not should_run_now(args.tz, args.run_times):
        if args.baseline_on_csv_change and csv_changed:
            # allow baseline runs outside schedule
            pass
        else:
            print("Not a scheduled run time; exiting.")
            return

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    df = pd.read_csv(args.csv)

    # Expected columns from your Moxfield export:
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

    prev = load_snapshot(args.snapshot)
    prev_cards = (prev.get("cards") or {}) if isinstance(prev, dict) else {}

    # FX rate (GBP per EUR)
    rate = None
    try:
        rate = eur_to_gbp_rate()
    except Exception:
        rate = None

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()

    current: Dict[str, Any] = {
        "_meta": {
            "generated_at": now_iso,
            "eur_to_gbp": rate,
            "csv_sha256": csv_hash,
            "run_type": "scheduled",  # will be overwritten to "baseline" when needed
            "suppress_next_no_alerts": False,
        },
        "cards": {}
    }

    alerts: List[str] = []

    # Query Scryfall in batches of up to 75 identifiers
    for batch in chunk(identifiers, 75):
        payload = {"identifiers": batch}
        r = requests.post(SCRYFALL_COLLECTION_URL, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        cards = data.get("data", [])

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
                continue

            prices = c.get("prices", {}) or {}
            purchase = c.get("purchase_uris") or {}
            cardmarket_url = purchase.get("cardmarket")

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
                    "cardmarket_url": cardmarket_url,
                    "eur": eur,
                }

        time.sleep(0.12)

    curr_cards = current["cards"]

    # --- BASELINE RUN SHORT-CIRCUIT ---
    baseline_run = bool(args.baseline_on_csv_change and csv_changed)

    if baseline_run:
        current["_meta"]["run_type"] = "baseline"
        current["_meta"]["suppress_next_no_alerts"] = True
        save_snapshot(args.snapshot, current)

        if webhook:
            tz = ZoneInfo(args.tz)
            now_local = datetime.now(tz)
            discord_post(
                webhook,
                f"🧱 **Baseline updated** — collection CSV changed.\n"
                f"Time: {now_local.strftime('%Y-%m-%d %H:%M')} ({args.tz})\n"
                f"Alerts will resume on the next scheduled run (07:00 or 19:00)."
            )

        return


    def get_prev_eur(k: str) -> float | None:
        return safe_float(prev_cards.get(k, {}).get("eur"))

    for k, info in curr_cards.items():
        eur = safe_float(info.get("eur"))
        if eur is None or eur < args.min_price:
            continue

        prev_eur = get_prev_eur(k)
        if prev_eur is None or prev_eur <= 0:
            continue

        delta = eur - prev_eur
        pct = (delta / prev_eur) * 100.0

        gbp = (eur * rate) if (rate is not None) else None
        prev_gbp = (prev_eur * rate) if (rate is not None) else None

        links = "\n".join([u for u in [info.get("scryfall_uri"), info.get("cardmarket_url")] if u])

        if (pct >= args.spike_pct) or (delta >= args.spike_abs):
            y_line = f"Yesterday: €{prev_eur:.2f}" + (f" / £{prev_gbp:.2f}" if prev_gbp is not None else "")
            t_line = f"Today: €{eur:.2f}" + (f" / £{gbp:.2f}" if gbp is not None else "")
            alerts.append(
                f"📈 **PRICE SPIKE**\n"
                f"**{info['name']}** ({info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']})\n"
                f"{y_line}\n"
                f"{t_line} (**{pct:+.0f}%**, {delta:+.2f} EUR)\n"
                f"{links}"
            )

        if pct <= args.dip_pct:
            y_line = f"Yesterday: €{prev_eur:.2f}" + (f" / £{prev_gbp:.2f}" if prev_gbp is not None else "")
            t_line = f"Today: €{eur:.2f}" + (f" / £{gbp:.2f}" if gbp is not None else "")
            alerts.append(
                f"📉 **PRICE DIP**\n"
                f"**{info['name']}** ({info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']})\n"
                f"{y_line}\n"
                f"{t_line} (**{pct:+.0f}%**, {delta:+.2f} EUR)\n"
                f"{links}"
            )

    # Weekly full summary CSV + digest (default Sunday 19:00 UK time)
    weekly_written = False
    weekly_path = None
    weekly_digest = None

    if is_weekly_time(args.tz, args.weekly_day, args.weekly_time):
        tz = ZoneInfo(args.tz)
        now_local = datetime.now(tz)
        stamp = now_local.strftime("%Y-%m-%d")
        weekly_path = f"data/weekly/weekly_summary_{stamp}.csv"
        write_weekly_summary_csv(
            out_path=weekly_path,
            cards=curr_cards,
            rate_gbp_per_eur=rate,
            prev_cards=prev_cards,
        )
        weekly_written = True
        weekly_digest = build_weekly_digest(
            curr_cards=curr_cards,
            prev_cards=prev_cards,
            rate_gbp_per_eur=rate,
            min_price=args.min_price,
            top_n=10,
        )

    # Post to Discord
    if webhook:
        tz = ZoneInfo(args.tz)
        now_local = datetime.now(tz)
        if rate is not None:
            header = (
                f"🧾 MTG price watch — {now_local.strftime('%Y-%m-%d %H:%M')} ({args.tz})\n"
                f"FX: 1 EUR = {rate:.4f} GBP"
            )
        else:
            header = (
                f"🧾 MTG price watch — {now_local.strftime('%Y-%m-%d %H:%M')} ({args.tz})\n"
                f"FX: unavailable"
            )

        if alerts:
            discord_post(webhook, header + f"\nAlerts: {len(alerts)}")
            msg = ""
            for a in alerts:
                if len(msg) + len(a) + 2 > 1800:
                    discord_post(webhook, msg)
                    msg = a
                else:
                    msg = (msg + "\n\n" + a).strip()
            if msg:
                discord_post(webhook, msg)
        else:
            # If the previous run was a baseline, don't spam a pointless "No alerts today"
            # on the very next scheduled run.
            if prev_was_baseline:
                print("Previous run was baseline; suppressing 'No alerts today' message.")
            else:
                # Suppress "No alerts today" exactly once after a baseline, then clear the flag.
                if prev_suppress_next_no_alerts:
                    print("Suppressing 'No alerts today' once (post-baseline).")
                    current["_meta"]["suppress_next_no_alerts"] = False
                else:
                discord_post(webhook, header + "\nNo alerts today.")

        if weekly_written and weekly_path:
            discord_post(webhook, f"📊 Weekly summary written: `{weekly_path}` (committed by workflow).")
            if weekly_digest:
                if len(weekly_digest) <= 1800:
                    discord_post(webhook, weekly_digest)
                else:
                    chunk_msg = ""
                    for line in weekly_digest.splitlines():
                        if len(chunk_msg) + len(line) + 1 > 1800:
                            discord_post(webhook, chunk_msg)
                            chunk_msg = line
                        else:
                            chunk_msg = (chunk_msg + "\n" + line).strip()
                    if chunk_msg:
                        discord_post(webhook, chunk_msg)

    # Save snapshot for next run
    save_snapshot(args.snapshot, current)


if __name__ == "__main__":
    main()
