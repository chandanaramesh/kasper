# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "044247a5-fb98-45fb-802d-251b33132a5d",
# META       "default_lakehouse_name": "Shortcut_Lakehouse",
# META       "default_lakehouse_workspace_id": "71235bdf-644e-40cc-8b23-78beaff4f80e",
# META       "known_lakehouses": [
# META         {
# META           "id": "044247a5-fb98-45fb-802d-251b33132a5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.sql("SELECT * FROM Shortcut_Lakehouse.dimension_customer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
