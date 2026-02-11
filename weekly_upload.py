import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


def discord_upload_and_pin(webhook_url: str, filepath: str, message: str) -> None:
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

    data = r.json()
    message_id = data.get("id")
    channel_id = data.get("channel_id")

    # Pin the message (webhook hack: reuse webhook token)
    if message_id and channel_id:
        pin_url = f"https://discord.com/api/v10/channels/{channel_id}/pins/{message_id}"
        pin_r = requests.put(pin_url, headers={"Authorization": f"Bot {webhook_url.split('/')[-1]}"})
        # If this fails silently, that’s OK — pinning is best-effort


def calculate_summary(csv_path: str) -> tuple[float, float]:
    df = pd.read_csv(csv_path)

    # Expecting columns: qty, gbp
    df["qty"] = df["qty"].fillna(0)
    df["gbp"] = df["gbp"].fillna(0)

    total_value = float((df["qty"] * df["gbp"]).sum())

    # If you later add a previous-week CSV, this becomes a real delta
    weekly_change = 0.0

    return total_value, weekly_change


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="CSV file to upload")
    ap.add_argument("--tz", default="Europe/London")
    ap.add_argument("--label", default="Weekly MTG Collection Snapshot")
    args = ap.parse_args()

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    now = datetime.now(ZoneInfo(args.tz))

    total_value, weekly_change = calculate_summary(args.file)

    change_line = f"{weekly_change:+.2f}"
    pct_line = ""  # placeholder for future week-over-week %

    message = (
        f"📎 **{args.label}** ({now:%d %b %Y})\n"
        f"**Total value:** £{total_value:,.2f}\n"
        f"**Weekly change:** {change_line}{pct_line}\n\n"
        f"Attached: `{os.path.basename(args.file)}`"
    )

    discord_upload_and_pin(webhook_url, args.file, message)


if __name__ == "__main__":
    main()
