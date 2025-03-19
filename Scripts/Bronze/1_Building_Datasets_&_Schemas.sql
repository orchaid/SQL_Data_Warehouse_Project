/*
==============================================================
Creating the DataWarehouse Database and Schemas
==============================================================
Script Purpose:
    - Create the DataWarehouse database
    - Create the bronze, silver, and gold schemas
    - If the database already exists, it will not be created again

-- you can create them via the visual interface of pgAdmin or other admin tools
-- or you can run the following script to create the database and schemas
*/


DO $$ 
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database 
      WHERE datname = 'datawarehouse'
   ) THEN
      CREATE DATABASE "DataWarehouse";
   END IF;
END $$;



CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
