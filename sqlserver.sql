CREATE DATABASE testDB
GO
CREATE LOGIN test WITH PASSWORD = 'TestPwd@345'
GO
USE testDB
GO
CREATE USER test FOR LOGIN test
GO
EXEC sp_addrolemember 'db_owner', test
GO
