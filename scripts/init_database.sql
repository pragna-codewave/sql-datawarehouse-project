/*
============================================================
Create Database and schemas
Notes:
   This script creates the DataWarehouse database and 
   the three schemas (bronze, silver, gold) to structure 
   the data flow.
============================================================
*/


USE Master;
Go

-- Create Database Datawarehouse
  
CREATE DATABASE DataWarehouse;

USE DataWarehouse

--Create Schemas
  
CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
Go
CREATE SCHEMA gold;
Go
