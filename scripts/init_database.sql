/*
=======================================================================
Create Database and Schemas
=======================================================================
Script Purpose:
   This script creates a new database named  'DataWarehouse' after checking if it already exists.
   If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
   within the database: 'bronze', 'silver', 'gold'.

WARNING:
    Running this script will drop the entire  'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.

*/
--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;
GO

use master;

--create database name it DatabaseWarehouse
create database DatabaseWarehouse;
GO
USE DatabaseWarehouse;

--create schemas here in our project 3 bronze, silver, gold ; to find go to db-security(sicherheit)-schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO   --its a batch seperator in sql to run to commands at a time
CREATE SCHEMA gold;
