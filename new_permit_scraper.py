#!/usr/bin/env python3
"""Report new Seattle building permits, either as they are issued or as they are applied for.

Each run asks Seattle's open data API for permits issued (or, with
--event applied, applied for) in the last N days, compares them with the permit
numbers recorded by earlier runs, appends any new ones to a CSV, and prints
them. With --email-to it also emails the list to one or more addresses. It
prints and sends nothing when there is nothing new, so it stays quiet under cron.

The two events keep separate records in --dir, so switching between them never
hides a permit:
    --event issued    seen.json          new_permits.csv
    --event applied   seen_applied.json  new_applications.csv

Example crontab entry (daily at 7:15):
    15 7 * * * /usr/bin/python3 /path/to/seattle_new_permits.py --dir /var/lib/seattle-permits --email-to you@example.com

Environment variables:
    SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASSWORD
        Mail server settings, required for --email-to. Port 465 uses SSL;
        any other port uses STARTTLS when the server offers it.
    SOCRATA_APP_TOKEN
        Optional. Avoids anonymous throttling by the open data API.
"""

import argparse
import csv
import io
import json
import os
import smtplib
import sys
import time
from datetime import date, timedelta
from email.message import EmailMessage
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "https://cos-data.seattle.gov/resource/76t5-zqzr.json"
PORTAL = "https://services.seattle.gov/portal/customize/LinkToRecord.aspx?altId="
PAGE_SIZE = 10000

# What "new" means for each --event: the date column to watch, where its
# records are kept, and how to word the output.
EVENTS = {
    "issued": {
        "date_field": "issueddate",
        "seen_file": "seen.json",
        "csv_file": "new_permits.csv",
        "verb": "issued",
        "heading": "newly issued Seattle permit(s)",
    },
    "applied": {
        "date_field": "applieddate",
        "seen_file": "seen_applied.json",
        "csv_file": "new_applications.csv",
        "verb": "applied",
        "heading": "new Seattle permit application(s)",
    },
}

# Columns written to the CSV. All but the first and last come straight
# from the dataset; first_seen and portal_link are added by this script.
DATASET_FIELDS = [
    "permitnum", "issueddate", "applieddate", "statuscurrent",
    "permitclass", "permittypemapped", "permittypedesc", "description",
    "estprojectcost", "originaladdress1", "originalzip",
    "contractorcompanyname", "relatedmup",
]
OUTPUT_FIELDS = ["first_seen"] + DATASET_FIELDS + ["portal_link"]


def get_json(url):
    """GET a URL and parse JSON, retrying transient failures."""
    headers = {"User-Agent": "seattle-new-permits/1.0", "Accept": "application/json"}
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    for attempt in range(3):
        try:
            with urlopen(Request(url, headers=headers), timeout=60) as resp:
                return json.load(resp)
        except HTTPError as err:
            # 4xx (other than rate limiting) means the request itself is wrong.
            if err.code < 500 and err.code != 429:
                detail = err.read().decode("utf-8", "replace")[:500]
                raise SystemExit(f"API rejected the request ({err.code}): {detail}")
            last_error = err
        except (URLError, TimeoutError) as err:
            last_error = err
        if attempt < 2:
            time.sleep(5 * (attempt + 1))
    raise SystemExit(f"API request failed after 3 attempts: {last_error}")


def fetch_since(date_field, cutoff, extra_where):
    """Return all permits whose `date_field` is on or after `cutoff`."""
    where = f"{date_field} >= '{cutoff.isoformat()}T00:00:00'"
    if extra_where:
        where += f" AND ({extra_where})"

    rows, offset = [], 0
    while True:
        params = {
            "$where": where,
            "$order": f"{date_field}, permitnum",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        page = get_json(f"{API}?{urlencode(params)}")
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows
        offset += PAGE_SIZE


def load_seen(path):
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_seen(path, seen):
    """Write atomically so an interrupted run can't corrupt the state file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seen, f)
    os.replace(tmp, path)


def to_output_row(permit, today):
    row = {field: permit.get(field, "") for field in DATASET_FIELDS}
    for field in ("issueddate", "applieddate"):
        row[field] = row[field][:10]  # drop the T00:00:00.000 suffix
    row["first_seen"] = today.isoformat()
    row["portal_link"] = PORTAL + permit["permitnum"]
    return row


def write_csv(f, rows, header=True):
    writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
    if header:
        writer.writeheader()
    writer.writerows(rows)


def append_rows(path, rows):
    is_new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        write_csv(f, rows, header=is_new_file)


def format_summary(rows, event):
    """Plain-text listing used for both stdout and the email body."""
    lines = []
    for r in rows:
        lines.append(f"{r['permitnum']}  {event['verb']} {r[event['date_field']]}  {r['originaladdress1']}")
        lines.append(f"    {r['permitclass']} / {r['permittypedesc'] or r['permittypemapped']}: {r['description'][:140]}")
        if event["verb"] == "applied" and r["statuscurrent"]:
            lines.append(f"    Status: {r['statuscurrent']}")
        if r["contractorcompanyname"]:
            lines.append(f"    Contractor: {r['contractorcompanyname']}")
        lines.append(f"    {r['portal_link']}")
    return "\n".join(lines)


def smtp_settings():
    """Read mail server settings from the environment, failing early if unusable."""
    host = os.environ.get("SMTP_HOST")
    if not host:
        raise SystemExit("--email-to needs SMTP_HOST set in the environment (see the top of this script)")
    return {
        "host": host,
        "port": int(os.environ.get("SMTP_PORT", "587")),
        "user": os.environ.get("SMTP_USER"),
        "password": os.environ.get("SMTP_PASSWORD", ""),
    }


def send_email(smtp_cfg, to_addrs, from_addr, rows, today, event):
    """Email the new permits: a listing in the body, full rows as a CSV attachment."""
    msg = EmailMessage()
    msg["Subject"] = f"{len(rows)} {event['heading']} - {today.isoformat()}"
    msg["From"] = from_addr or smtp_cfg["user"] or to_addrs[0]
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(format_summary(rows, event) + "\n")

    attachment = io.StringIO()
    write_csv(attachment, rows)
    msg.add_attachment(attachment.getvalue(), subtype="csv", filename=f"{event['csv_file'][:-4]}_{today.isoformat()}.csv")

    use_ssl = smtp_cfg["port"] == 465
    smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_class(smtp_cfg["host"], smtp_cfg["port"], timeout=60) as smtp:
        encrypted = use_ssl
        if not use_ssl:
            smtp.ehlo()
            if smtp.has_extn("starttls"):
                smtp.starttls()
                smtp.ehlo()
                encrypted = True
        if smtp_cfg["user"]:
            if not encrypted:
                raise SystemExit("refusing to send SMTP credentials over an unencrypted connection")
            smtp.login(smtp_cfg["user"], smtp_cfg["password"])
        smtp.send_message(msg)


def main():
    parser = argparse.ArgumentParser(description="Report new Seattle building permits.")
    parser.add_argument(
        "--event", choices=sorted(EVENTS), default="issued",
        help="report permits when they are issued (default) or when they are applied for",
    )
    parser.add_argument(
        "--dir", default=os.path.expanduser("~/.seattle-permits"),
        help="where to keep the seen-permits record and the CSV (default: ~/.seattle-permits)",
    )
    parser.add_argument(
        "--lookback", type=int, default=30, metavar="DAYS",
        help="how far back to query by issue or application date on each run (default: 30)",
    )
    parser.add_argument(
        "--where", default="", metavar="SOQL",
        help="extra filter, e.g. \"permittypemapped = 'Building' AND permitclassmapped = 'Non-Residential'\"",
    )
    parser.add_argument(
        "--email-to", nargs="+", action="extend", default=[], metavar="ADDRESS",
        help="email the new permits to these addresses (space- or comma-separated; may be repeated)",
    )
    parser.add_argument("--email-from", metavar="ADDRESS", help="sender address (default: SMTP_USER)")
    args = parser.parse_args()

    # Accept "a@x.com b@y.com", "a@x.com,b@y.com", or a repeated flag; drop duplicates.
    recipients = list(dict.fromkeys(
        addr.strip() for value in args.email_to for addr in value.split(",") if addr.strip()
    ))
    for addr in recipients:
        if "@" not in addr:
            parser.error(f"--email-to: {addr!r} doesn't look like an email address")

    # Check mail settings up front so a misconfiguration shows on the first run,
    # not on the first day there happens to be a new permit.
    smtp_cfg = smtp_settings() if recipients else None

    os.makedirs(args.dir, exist_ok=True)
    event = EVENTS[args.event]
    seen_path = os.path.join(args.dir, event["seen_file"])
    csv_path = os.path.join(args.dir, event["csv_file"])

    today = date.today()
    seen = load_seen(seen_path)
    permits = fetch_since(event["date_field"], today - timedelta(days=args.lookback), args.where)

    # Keyed by permit number so a record repeated across pages is only counted once.
    new = {p["permitnum"]: p for p in permits if p.get("permitnum") and p["permitnum"] not in seen}
    if not new:
        return

    rows = [to_output_row(p, today) for p in new.values()]

    # Send before recording anything: if the email fails, the run exits with an
    # error and the same permits are picked up again next time.
    if smtp_cfg:
        try:
            send_email(smtp_cfg, recipients, args.email_from, rows, today, event)
        except OSError as err:  # includes smtplib errors
            raise SystemExit(f"could not send email via {smtp_cfg['host']}:{smtp_cfg['port']}: {err}")

    append_rows(csv_path, rows)
    seen.update({r["permitnum"]: r[event["date_field"]] for r in rows})
    save_seen(seen_path, seen)

    print(f"{len(rows)} {event['heading']}, appended to {csv_path}\n")
    print(format_summary(rows, event))


if __name__ == "__main__":
    try:
        main()
    except OSError as err:
        sys.exit(f"error: {err}")
