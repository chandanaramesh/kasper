-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "6c38c8fb-2052-b927-45b0-fc17ec90a5f5",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "6c38c8fb-2052-b927-45b0-fc17ec90a5f5",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [CityKey],
			[WWICityID],
			[City],
			[StateProvince],
			[Country],
			[Continent],
			[SalesTerritory],
			[Region],
			[Subregion],
			[Location],
			[LatestRecordedPopulation],
			[ValidFrom],
			[ValidTo],
			[LineageKey]
FROM [Wide World Importers].[dbo].[dimension_city]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
