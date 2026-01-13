-- Creates table if it doesn't exist and loads the LUTAssetTypes data into it

CREATE TABLE IF NOT EXISTS LUTAssetTypes (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  alternativename TEXT NOT NULL,
  geometrytype TEXT NOT NULL,
  geometrytype_corrected TEXT NOT NULL
);

-- Load CSV into the table using COPY, and skips 1st row header
COPY LUTAssetTypes(id, name)
FROM '/docker-entrypoint-initdb.d/LUTAssetTypes.csv'
DELIMITER ','
CSV HEADER;
