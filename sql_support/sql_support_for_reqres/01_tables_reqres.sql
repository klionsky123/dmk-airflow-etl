
-- in raw tables - ALL COLUMNS ARE NVARCHAR, to avoid conversion errors
drop table if exists raw.reqres_user
drop table if exists dbo.reqres_user
drop table if exists dbo.reqres_resource
drop table if exists raw.reqres_resource

CREATE TABLE raw.reqres_user (
    id INT PRIMARY KEY,
    email NVARCHAR(100),
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    avatar NVARCHAR(200),
    support_url NVARCHAR(200),
    support_text NVARCHAR(MAX),
	job_inst_id int not null,
	date_created smalldatetime DEFAULT GETDATE()
);


CREATE TABLE dbo.reqres_user (
id INT PRIMARY KEY,
    email VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    avatar VARCHAR(200),
    support_url VARCHAR(200),
    support_text VARCHAR(MAX),
	job_inst_id int not null,
    insert_utc DATETIME DEFAULT GETUTCDATE(),
	date_created smalldatetime DEFAULT GETDATE()
);

CREATE TABLE [raw].reqres_resource (
    id INT primary key,
    name NVARCHAR(100),
    year INT,
    color NVARCHAR(50),
    pantone_value NVARCHAR(50),
	page INT,
    per_page INT,
    total INT,
    total_pages INT,
    support_url NVARCHAR(200),
    support_text NVARCHAR(MAX),
    job_inst_id INT,
    load_utc DATETIME DEFAULT SYSUTCDATETIME()
);

CREATE TABLE [dbo].reqres_resource (
    id INT primary key,
    name VARCHAR(100),
    year INT,
    color VARCHAR(50),
    pantone_value VARCHAR(50),
	page INT,
    per_page INT,
    total INT,
    total_pages INT,
    support_url VARCHAR(200),
    support_text VARCHAR(MAX),
    job_inst_id INT,
    insert_utc DATETIME DEFAULT GETUTCDATE(),
	date_created smalldatetime DEFAULT GETDATE()
);