import argparse
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import pandas as pd
import requests
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
    return [items[i:i + n] for i in range(0, len(items), n)]


def load_snapshot(path: str) -> Dict[str, Any]:
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
    Only run at those exact local times (e.g. 07:00,19:00)
    """
    if not run_times_csv.strip():
        return True
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    now_hm = now_local.strftime("%H:%M")
    allowed = {t.strip() for t in run_times_csv.split(",") if t.strip()}
    return now_hm in allowed


def parse_weekday(s: str) -> int:
    days = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
    s = (s or "").strip().upper()
    return days.get(s, 6)


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


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for ch in iter(lambda: f.read(1024 * 1024), b""):
            h.update(ch)
    return h.hexdigest()


def parse_csv_list(csv_arg: str) -> List[str]:
    # Allow: --csv collection/moxfield.csv OR --csv a.csv,b.csv
    parts = [p.strip() for p in csv_arg.split(",") if p.strip()]
    return parts


def read_collection_csvs(csv_paths: List[str]) -> pd.DataFrame:
    dfs = []
    for p in csv_paths:
        if not os.path.exists(p):
            raise SystemExit(f"CSV not found: {p}")
        df = pd.read_csv(p)
        df["__source_csv"] = p
        dfs.append(df)
    if not dfs:
        raise SystemExit("No CSV files provided.")
    return pd.concat(dfs, ignore_index=True)


def reprint_risk(info: Dict[str, Any]) -> str:
    """
    Heuristic risk tagging:
    - Reserved list => Very Low
    - Old (pre-2003) => Low
    - Modern era => Medium
    - Recently reprinted heuristic is hard without more data; keep it simple.
    """
    if info.get("reserved_list") is True:
        return "Very Low (RL)"
    year = info.get("released_year")
    if isinstance(year, int):
        if year <= 2003:
            return "Low (Older printing)"
        if year <= 2015:
            return "Medium"
        return "Medium/High"
    return "Unknown"


def fmt_money_gbp_first(eur: float | None, gbp: float | None) -> str:
    if gbp is not None and eur is not None:
        return f"£{gbp:.2f} (€{eur:.2f})"
    if gbp is not None:
        return f"£{gbp:.2f}"
    if eur is not None:
        return f"€{eur:.2f}"
    return "n/a"


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
            "risk": info.get("risk"),
            "reserved_list": info.get("reserved_list"),
            "released_year": info.get("released_year"),
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
        gbp_part = f"£{delta_gbp:+.2f}" if delta_gbp is not None else "n/a"
        return f"- **{name}** ({tag}) — **{pct:+.0f}%** (Δ {gbp_part}, Δ€{delta_eur:+.2f}) · Risk: {info.get('risk','?')}\n  {links}"

    lines = ["📊 **Weekly digest (your collection)**", "", "**Top gainers**"]
    for pct, d_eur, d_gbp, info in gainers:
        lines.append(fmt_line(pct, d_eur, d_gbp, info))

    lines += ["", "**Top losers**"]
    for pct, d_eur, d_gbp, info in losers:
        lines.append(fmt_line(pct, d_eur, d_gbp, info))

    return "\n".join(lines)


def build_weekly_what_to_list(
    curr_cards: Dict[str, Any],
    prev_cards: Dict[str, Any],
    rate_gbp_per_eur: float | None,
    min_price: float,
    min_pct: float,
    min_abs_gbp: float,
    top_n: int = 10,
) -> str:
    """
    Weekly seller-focused summary:
    - "worth listing" if up >= min_pct OR up >= min_abs_gbp in GBP terms
    """
    candidates = []
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

        is_candidate = (pct >= min_pct) or (delta_gbp is not None and delta_gbp >= min_abs_gbp)
        if not is_candidate:
            continue

        score = pct  # simple sort score
        candidates.append((score, pct, delta_gbp, info, gbp, eur))

    if not candidates:
        return "🧾 **Weekly: what to list**\nNo clear “list now” candidates this week."

    candidates.sort(key=lambda x: x[0], reverse=True)
    candidates = candidates[:top_n]

    lines = ["🧾 **Weekly: what to list (your collection)**"]
    for _, pct, d_gbp, info, gbp, eur in candidates:
        tag = f"{info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']}"
        links = " | ".join([u for u in [info.get("scryfall_uri"), info.get("cardmarket_url")] if u])
        d_gbp_txt = f"{d_gbp:+.2f}" if d_gbp is not None else "n/a"
        lines.append(
            f"- **{info['name']}** ({tag}) — now {fmt_money_gbp_first(eur, gbp)} "
            f"(~Δ£{d_gbp_txt}, {pct:+.0f}%) · Risk: {info.get('risk','?')}\n  {links}"
        )

    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path(s) to Moxfield export CSV. Single file or comma-separated list.")
    ap.add_argument("--snapshot", default="data/last_prices.json", help="Where to store last run prices")
    ap.add_argument("--spike_pct", type=float, default=30.0, help="Spike threshold percent (day-over-day)")
    ap.add_argument("--spike_abs_eur", type=float, default=2.0, help="Spike threshold absolute EUR increase")
    ap.add_argument("--dip_pct", type=float, default=-25.0, help="Dip threshold percent (negative)")
    ap.add_argument("--min_price_eur", type=float, default=1.5, help="Ignore cards below this EUR price")
    ap.add_argument("--tz", default="Europe/London", help="Timezone for run gating, e.g. Europe/London")
    ap.add_argument("--run-times", default="07:00,19:00", help="Comma-separated local times to run, e.g. 07:00,19:00")
    ap.add_argument("--weekly-day", default="SUN", help="Weekly summary day (MON..SUN), default SUN")
    ap.add_argument("--weekly-time", default="19:00", help="Weekly summary local time, default 19:00")
    ap.add_argument("--baseline-on-csv-change", action="store_true", help="If CSV changed, run baseline snapshot update and skip alerts")
    # Sell / buy signals
    ap.add_argument("--sell_candidate_pct", type=float, default=80.0, help="Sell-candidate threshold percent gain")
    ap.add_argument("--sell_candidate_abs_gbp", type=float, default=5.0, help="Sell-candidate threshold absolute GBP gain")
    ap.add_argument("--buy_more_pct", type=float, default=-30.0, help="Buy-more signal threshold percent drop (negative)")
    # Weekly list-now report thresholds
    ap.add_argument("--weekly_list_pct", type=float, default=50.0, help="Weekly list-now pct threshold")
    ap.add_argument("--weekly_list_abs_gbp", type=float, default=4.0, help="Weekly list-now abs GBP threshold")
    args = ap.parse_args()

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    csv_paths = parse_csv_list(args.csv)
    csv_hash = hashlib.sha256(("|".join([p + ":" + file_sha256(p) for p in csv_paths])).encode("utf-8")).hexdigest()

    prev = load_snapshot(args.snapshot)
    prev_cards = (prev.get("cards") or {}) if isinstance(prev, dict) else {}

    prev_suppress_next_no_alerts = False
    try:
        prev_suppress_next_no_alerts = bool(prev.get("_meta", {}).get("suppress_next_no_alerts"))
    except Exception:
        prev_suppress_next_no_alerts = False

    prev_hash = None
    try:
        prev_hash = prev.get("_meta", {}).get("csv_sha256")
    except Exception:
        prev_hash = None

    csv_changed = (prev_hash != csv_hash)

    # Gate to run times unless this is a baseline run caused by CSV change.
    if not should_run_now(args.tz, args.run_times):
        if args.baseline_on_csv_change and csv_changed:
            pass
        else:
            print("Not a scheduled run time; exiting.")
            return

    # FX rate (GBP per EUR)
    rate = None
    try:
        rate = eur_to_gbp_rate()
    except Exception:
        rate = None

    # Read & combine collection CSV(s)
    df = read_collection_csvs(csv_paths)

    required = ["Count", "Name", "Edition", "Collector Number", "Language", "Foil"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

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
        ident = {"set": row["set_code"], "collector_number": row["collector"], "lang": row["lang_code"]}
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

    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()

    current: Dict[str, Any] = {
        "_meta": {
            "generated_at": now_iso,
            "eur_to_gbp": rate,
            "csv_sha256": csv_hash,
            "run_type": "scheduled",
            "suppress_next_no_alerts": False,
        },
        "cards": {}
    }

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

            released_at = c.get("released_at")  # YYYY-MM-DD
            released_year = None
            try:
                if isinstance(released_at, str) and len(released_at) >= 4:
                    released_year = int(released_at[:4])
            except Exception:
                released_year = None

            reserved_list = bool(c.get("reserved")) if c.get("reserved") is not None else False

            base_key_prefix = f"{sc}|{cn}|{lang}|"
            for kind in ("nonfoil", "foil", "etched"):
                k = base_key_prefix + kind
                meta = key_to_meta.get(k)
                if not meta:
                    continue
                eur = pick_price_eur(prices, kind)
                info = {
                    **meta,
                    "scryfall_uri": c.get("scryfall_uri"),
                    "cardmarket_url": cardmarket_url,
                    "eur": eur,
                    "released_year": released_year,
                    "reserved_list": reserved_list,
                }
                info["risk"] = reprint_risk(info)
                current["cards"][k] = info

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

    alerts: List[str] = []
    sell_candidates: List[str] = []
    buy_more_signals: List[str] = []

    for k, info in curr_cards.items():
        eur = safe_float(info.get("eur"))
        if eur is None or eur < args.min_price_eur:
            continue

        prev_eur = get_prev_eur(k)
        if prev_eur is None or prev_eur <= 0:
            continue

        delta_eur = eur - prev_eur
        pct = (delta_eur / prev_eur) * 100.0

        gbp = (eur * rate) if (rate is not None) else None
        prev_gbp = (prev_eur * rate) if (rate is not None) else None
        delta_gbp = (gbp - prev_gbp) if (gbp is not None and prev_gbp is not None) else None

        links = "\n".join([u for u in [info.get("scryfall_uri"), info.get("cardmarket_url")] if u])
        tag = f"{info['set'].upper()} #{info['collector_number']} · {info['foil_kind']} · x{info['qty']}"
        money_now = fmt_money_gbp_first(eur, gbp)
        money_prev = fmt_money_gbp_first(prev_eur, prev_gbp)

        # Base spike/dip alerts (your original behaviour)
        if (pct >= args.spike_pct) or (delta_eur >= args.spike_abs_eur):
            alerts.append(
                f"📈 **PRICE SPIKE**\n"
                f"**{info['name']}** ({tag})\n"
                f"Yesterday: {money_prev}\n"
                f"Today: {money_now} (**{pct:+.0f}%**, Δ€{delta_eur:+.2f})\n"
                f"Risk: {info.get('risk','?')}\n"
                f"{links}"
            )

        if pct <= args.dip_pct:
            alerts.append(
                f"📉 **PRICE DIP**\n"
                f"**{info['name']}** ({tag})\n"
                f"Yesterday: {money_prev}\n"
                f"Today: {money_now} (**{pct:+.0f}%**, Δ€{delta_eur:+.2f})\n"
                f"Risk: {info.get('risk','?')}\n"
                f"{links}"
            )

        # Sell candidate (strong move)
        is_sell = (pct >= args.sell_candidate_pct) or (delta_gbp is not None and delta_gbp >= args.sell_candidate_abs_gbp)
        if is_sell:
            dgbp = f"{delta_gbp:+.2f}" if delta_gbp is not None else "n/a"
            sell_candidates.append(
                f"💰 **SELL CANDIDATE**\n"
                f"**{info['name']}** ({tag})\n"
                f"Now: {money_now} (Δ£{dgbp}, {pct:+.0f}%)\n"
                f"Risk: {info.get('risk','?')}\n"
                f"{links}"
            )

        # Buy-more signal (big dip)
        if pct <= args.buy_more_pct:
            dgbp = f"{delta_gbp:+.2f}" if delta_gbp is not None else "n/a"
            buy_more_signals.append(
                f"🛒 **BUY-MORE SIGNAL**\n"
                f"**{info['name']}** ({tag})\n"
                f"Now: {money_now} (Δ£{dgbp}, {pct:+.0f}%)\n"
                f"Risk: {info.get('risk','?')}\n"
                f"{links}"
            )

    # Weekly full summary CSV + digests
    weekly_written = False
    weekly_path = None
    weekly_digest = None
    weekly_list_report = None

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
            min_price=args.min_price_eur,
            top_n=10,
        )
        weekly_list_report = build_weekly_what_to_list(
            curr_cards=curr_cards,
            prev_cards=prev_cards,
            rate_gbp_per_eur=rate,
            min_price=args.min_price_eur,
            min_pct=args.weekly_list_pct,
            min_abs_gbp=args.weekly_list_abs_gbp,
            top_n=10,
        )

    # Discord posting
    if webhook:
        tz = ZoneInfo(args.tz)
        now_local = datetime.now(tz)

        fx_line = f"FX: 1 EUR = {rate:.4f} GBP" if rate is not None else "FX: unavailable"
        header = f"🧾 MTG price watch — {now_local.strftime('%Y-%m-%d %H:%M')} ({args.tz})\n{fx_line}"

        # Sell candidates first (actionable)
        if sell_candidates:
            discord_post(webhook, header + f"\nSell candidates: {len(sell_candidates)}")
            msg = ""
            for a in sell_candidates:
                if len(msg) + len(a) + 2 > 1800:
                    discord_post(webhook, msg)
                    msg = a
                else:
                    msg = (msg + "\n\n" + a).strip()
            if msg:
                discord_post(webhook, msg)

        # Buy-more signals
        if buy_more_signals:
            discord_post(webhook, header + f"\nBuy-more signals: {len(buy_more_signals)}")
            msg = ""
            for a in buy_more_signals:
                if len(msg) + len(a) + 2 > 1800:
                    discord_post(webhook, msg)
                    msg = a
                else:
                    msg = (msg + "\n\n" + a).strip()
            if msg:
                discord_post(webhook, msg)

        # Regular spike/dip alerts
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
            # Suppress "No alerts today" exactly once after a baseline, then clear the flag.
            if prev_suppress_next_no_alerts:
                print("Suppressing 'No alerts today' once (post-baseline).")
                current["_meta"]["suppress_next_no_alerts"] = False
            else:
                discord_post(webhook, header + "\nNo alerts today.")

        # Weekly extras
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

            if weekly_list_report:
                if len(weekly_list_report) <= 1800:
                    discord_post(webhook, weekly_list_report)
                else:
                    chunk_msg = ""
                    for line in weekly_list_report.splitlines():
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
