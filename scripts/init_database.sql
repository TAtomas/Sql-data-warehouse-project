/*
=============================================================
Database and Schema Initialization Script
=============================================================
Description:
    This script creates a new database named 'DataWarehouse'. 
    It first checks if the database already exists; if found, it drops 
    the existing database and recreates it from scratch. 

    Once the database is created, the script defines three schemas: 
    'bronze', 'silver', and 'gold' — representing different data 
    processing layers.

Note:
    Executing this script will completely remove the existing 
    'DataWarehouse' database, including all its data and objects. 
    Ensure that backups are taken before running this script, 
    as data loss will be irreversible.
*/




use master;
Go
  -- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;


create database DataWareHouse;
Go
  
use DataWareHouse;
Go
  
Create Schema bronze;
Go
  
Create Schema silver;
Go
  
Create Schema gold;
GO



