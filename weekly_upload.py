import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="CSV file to upload")
    ap.add_argument("--tz", default="Europe/London")
    ap.add_argument("--label", default="Weekly MTG Collection Snapshot")
    args = ap.parse_args()

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    now = datetime.now(ZoneInfo(args.tz))
    msg = f"📎 **{args.label}** ({now:%d %b %Y})\nAttached: `{os.path.basename(args.file)}`"

    discord_upload_file(webhook_url, args.file, msg)


if __name__ == "__main__":
    main()
