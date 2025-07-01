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

import os, requests, textwrap
# -- config (parameterised) --
raw_url = "https://github.com/chandanaramesh/kasper/blob/main/2019.csv"
target_path = "/lakehouse/default/Files/config/etl_metadata.csv"

# -- skip if already present (idempotent) --
if not os.path.exists(target_path):
    token = dbutils.secrets.get("fabric-kv", "git-pat")        # optional
    headers = {"Authorization": f"token {token}"} if token else {}
    r = requests.get(raw_url, headers=headers, timeout=30)
    r.raise_for_status()
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "wb") as f:
        f.write(r.content)
    print("File downloaded ✔")
else:
    print("File already present, skipping.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
