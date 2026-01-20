#!/usr/bin/env python3
# scripts/run_harvest_from_sheet.py

import os, sys, argparse
import datetime
from datetime import date
from dotenv import load_dotenv


# -------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT, ".env"))

# ---- src/ import shim
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# -------------------------------------------------------------

from preprint_harvester.harvesters import harvest_servers_from_rules_sheet

# -------------------------------------------------------------
# Config from environment
# -------------------------------------------------------------
MAILTO = os.getenv("HARVESTER_MAILTO")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")

if not MAILTO:
    raise RuntimeError("HARVESTER_MAILTO is not set in .env")

print("MAILTO =", MAILTO)
print("OPENALEX_API_KEY loaded =", bool(OPENALEX_API_KEY))

# -------------------------------------------------------------
# Google Sheet
# -------------------------------------------------------------
sheet_id = "10_7FdcpZjntqFsEHIii7bAM72uF__of_iUohSD5w8w4"
gid = "1230415212"

SHEET_CSV = (
    f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    f"/export?format=csv&gid={gid}"
)

servers_to_test = [
    "Open Science Framework",
    "F1000Research",
    "Gates Open Research",
    "CERN document server",
    "CrossAsia-Repository (Universität Heidelberg)",
    "Organic Eprints",
    # "arXiv",
    'HAL', # 1990-2009 errors on harvest / 2012
    'RePEc: Research Papers in Economics',
]

DO_DRY_RUN = False

summary = harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url=SHEET_CSV,
    servers=None, # specify None to do all servers in the sheet # servers_to_test
    date_start="1990-01-01", # "1990-01-01"
    date_end="2022-03-02", #date.today().isoformat(),  "2025-12-31"
    mailto=MAILTO,
    output_root="data/by_server",
    dry_run=DO_DRY_RUN,
    openalex_api_key=OPENALEX_API_KEY,
)

print(summary)
