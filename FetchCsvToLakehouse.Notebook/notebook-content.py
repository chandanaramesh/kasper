# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "320fad9d-3f7e-4484-bef4-9e9f2f3abf55",
# META       "default_lakehouse_name": "LH_Dev",
# META       "default_lakehouse_workspace_id": "044ff1db-6e9f-46e9-8f63-48a5afd5a497",
# META       "known_lakehouses": [
# META         {
# META           "id": "320fad9d-3f7e-4484-bef4-9e9f2f3abf55"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ── 1. Imports ──────────────────────────────────────────────
import requests
from notebookutils import mssparkutils

# ── 2. CONFIG ‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑‑
RAW_URL = (
    # ⚠️  IMPORTANT: use the *raw* URL pattern, not the normal GitHub page URL
    # https://raw.githubusercontent.com/<owner>/<repo>/<branch>/path/to/file.csv
    "https://raw.githubusercontent.com/chandanaramesh/kasper/main/2019.csv"
)

# If your repo is PRIVATE, paste the classic PAT below; if it’s PUBLIC, leave it blank
GITHUB_PAT = ""                      # e.g. "ghp_k4sper1234567890abcDEF"
HEADERS = {"Authorization": f"token {GITHUB_PAT}"} if GITHUB_PAT else {}

# Lakehouse destination
DEST_PATH   = "Files/config/etl_metadata.csv"   # no leading slash
TEMP_FILE   = "/tmp/etl_metadata.csv"           # local scratch file

# ── 3. Download (only if not already in Lakehouse) ─────────
if not mssparkutils.fs.exists(DEST_PATH):
    # 3‑A  fetch bytes from GitHub
    resp = requests.get(RAW_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    # 3‑B  save to local /tmp
    with open(TEMP_FILE, "wb") as f:
        f.write(resp.content)

    # 3‑C  ensure Lakehouse folder exists once
    mssparkutils.fs.mkdirs("Files/config")

    # 3‑D  copy local → Lakehouse
    mssparkutils.fs.cp(f"file:{TEMP_FILE}", DEST_PATH)

    print("CSV copied to Lakehouse ✔")
else:
    print("CSV already present – skipping download")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
