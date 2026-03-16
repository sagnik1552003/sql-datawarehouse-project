/*Setting up project .creating database, setting up schemas*/

Use master;
create database Datawarehouse;
use Datawarehouse;

GO
create schema bronze;
GO
create schema silver;
GO
create schema gold;
