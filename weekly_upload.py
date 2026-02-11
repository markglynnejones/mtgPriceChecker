import argparse
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


SNAP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.csv$")


def discord_upload_file(webhook_url: str, filepath: str, message: str) -> None:
    if not webhook_url:
        raise SystemExit("DISCORD_WEBHOOK_URL is missing")
    if not os.path.exists(filepath):
        raise SystemExit(f"File not found: {filepath}")

    with open(filepath, "rb") as f:
        r = requests.post(
            webhook_url,
            data={"content": message},
            files={"file": (os.path.basename(filepath), f, "text/csv")},
            timeout=60,
        )

    if r.status_code >= 300:
        raise SystemExit(f"Discord upload failed ({r.status_code}): {r.text}")


def load_snapshot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # Normalise expected columns defensively
    for col in ["qty", "gbp", "eur"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "qty" not in df.columns:
        df["qty"] = 0
    df["qty"] = df["qty"].fillna(0)

    # Prefer GBP; fall back to EUR if GBP missing
    if "gbp" not in df.columns:
        df["gbp"] = None
    df["gbp"] = df["gbp"].fillna(df.get("eur", 0)).fillna(0)

    # A stable key to match cards between weeks
    for c in ["name", "set", "collector_number", "foil_kind", "lang"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].fillna("").astype(str)

    df["key"] = (
        df["name"].str.strip() + "||" +
        df["set"].str.strip() + "||" +
        df["collector_number"].str.strip() + "||" +
        df["foil_kind"].str.strip() + "||" +
        df["lang"].str.strip()
    )

    return df


def find_latest_and_prev(snapshots_dir: str) -> tuple[str | None, str | None]:
    if not snapshots_dir or not os.path.isdir(snapshots_dir):
        return None, None

    files = [
        f for f in os.listdir(snapshots_dir)
        if SNAP_RE.match(f)
    ]
    files.sort()
    if len(files) < 2:
        latest = os.path.join(snapshots_dir, files[-1]) if files else None
        return latest, None

    latest = os.path.join(snapshots_dir, files[-1])
    prev = os.path.join(snapshots_dir, files[-2])
    return latest, prev


def fmt_pct(x: float) -> str:
    return f"{x*100:+.2f}%"


def build_summary(latest_csv: str, snapshots_dir: str | None) -> tuple[str, str]:
    # Returns (summary_text, movers_text)
    latest_df = load_snapshot(latest_csv)
    latest_value = float((latest_df["qty"] * latest_df["gbp"]).sum())

    movers_text = ""
    change_text = "**Weekly change:** N/A (no previous snapshot found)"

    prev_csv = None
    if snapshots_dir:
        _, prev_csv = find_latest_and_prev(snapshots_dir)

    if prev_csv and os.path.exists(prev_csv):
        prev_df = load_snapshot(prev_csv)

        prev_value = float((prev_df["qty"] * prev_df["gbp"]).sum())
        delta_value = latest_value - prev_value
        delta_pct = (delta_value / prev_value) if prev_value else 0.0

        change_text = f"**Weekly change:** £{delta_value:+,.2f} ({fmt_pct(delta_pct)})"

        # Merge for per-card movement (impact on your collection)
        m = latest_df[["key", "name", "set", "collector_number", "foil_kind", "lang", "qty", "gbp"]].merge(
            prev_df[["key", "qty", "gbp"]].rename(columns={"qty": "qty_prev", "gbp": "gbp_prev"}),
            on="key",
            how="outer",
        )

        m["name"] = m["name"].fillna("")
        m["set"] = m["set"].fillna("")
        m["collector_number"] = m["collector_number"].fillna("")
        m["foil_kind"] = m["foil_kind"].fillna("")
        m["lang"] = m["lang"].fillna("")

        m["qty"] = m["qty"].fillna(0)
        m["gbp"] = m["gbp"].fillna(0)
        m["qty_prev"] = m["qty_prev"].fillna(0)
        m["gbp_prev"] = m["gbp_prev"].fillna(0)

        # Collection impact: assume current qty for impact (you can swap to min/avg if you prefer)
        m["delta_price"] = m["gbp"] - m["gbp_prev"]
        m["impact_value"] = m["qty"] * m["delta_price"]

        # A nicer display label
        def label(row) -> str:
            bits = [row["name"]]
            if row["set"]:
                bits.append(f"({row['set']})")
            if row["foil_kind"]:
                bits.append(row["foil_kind"])
            return " ".join([b for b in bits if b]).strip()

        m["label"] = m.apply(label, axis=1)

        # Top movers by value impact
        up = m.sort_values("impact_value", ascending=False).head(5)
        down = m.sort_values("impact_value", ascending=True).head(5)

        def format_movers(df: pd.DataFrame) -> str:
            lines = []
            for _, r in df.iterrows():
                if not r["label"]:
                    continue
                lines.append(f"- {r['label']}: £{float(r['impact_value']):+,.2f} (Δ £{float(r['delta_price']):+,.2f})")
            return "\n".join(lines) if lines else "- (none)"

        movers_text = (
            "\n\n**Top 5 risers (collection impact)**\n"
            f"{format_movers(up)}\n"
            "\n**Top 5 fallers (collection impact)**\n"
            f"{format_movers(down)}"
        )

    summary_text = f"**Total value:** £{latest_value:,.2f}\n{change_text}"
    return summary_text, movers_text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="CSV file to upload")
    ap.add_argument("--snapshots-dir", default="", help="Directory containing dated snapshots (YYYY-MM-DD.csv)")
    ap.add_argument("--tz", default="Europe/London")
    ap.add_argument("--label", default="Weekly MTG Collection Snapshot")
    args = ap.parse_args()

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    now = datetime.now(ZoneInfo(args.tz))

    summary_text, movers_text = build_summary(args.file, args.snapshots_dir or None)

    message = (
        f"📎 **{args.label}** ({now:%d %b %Y})\n"
        f"{summary_text}\n\n"
        f"Attached: `{os.path.basename(args.file)}`"
        f"{movers_text}"
    )

    discord_upload_file(webhook_url, args.file, message)


if __name__ == "__main__":
    main()
