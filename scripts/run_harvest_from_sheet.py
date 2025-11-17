#!/usr/bin/env python3
# scripts/run_harvest_from_sheet.py
import os, sys, argparse
from datetime import date

# ---- src/ import shim (so you don't need pip install -e . yet)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# -------------------------------------------------------------

import argparse
from preprint_harvester.harvesters import harvest_servers_from_rules_sheet



MAILTO = "dmbogning15@gmail.com"
SHEET_CSV = "https://docs.google.com/spreadsheets/d/1I8_bX7dBcc-MFmD9kXLeAmh8ULn-NvSH849Ncj315Sg/export?format=csv&gid=1730268225"

servers_to_test = [
    "Preprints.org",
    "Open Science Framework",
    "Authorea Inc.",
    "CERN document server",
    "Zenodo",
    'HAL (Le Centre pour la Communication Scientifique Directe)',
    'Arabixiv (OSF Preprints)',
]

DO_DRY_RUN = False  # ⬅️ mets False quand tu es prêt pour le vrai run

summary = harvest_servers_from_rules_sheet(
    sheet_csv_path_or_url=SHEET_CSV,
    servers=servers_to_test, #servers_to_test,     # ou None pour tous
    date_start="2025-10-01",
    date_end="2025-10-05",
    mailto=MAILTO,
    output_root="data/by_server",
    dry_run=DO_DRY_RUN,
)

summary
