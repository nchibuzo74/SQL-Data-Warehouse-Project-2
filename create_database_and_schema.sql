----Create database and schemas in PostgreSQL

---Create Database called DataWarehouse
drop database if exists DataWarehouse;
create database DataWarehouse;

---Create schema namely - bronze, silver and gold
drop schema if exists bronze;
create schema bronze;

drop schema if exists silver;
create schema silver;

drop schema if exists gold;
create schema gold;
