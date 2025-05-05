CREATE TABLE dmk_stage_db.raw.openalex_authors (
    id NVARCHAR(100) PRIMARY KEY,
    display_name NVARCHAR(500),
    orcid NVARCHAR(255),
    works_count INT,
    citation_count INT,
    last_known_institution NVARCHAR(500),
    created_date smalldatetime DEFAULT GETDATE()
);

go

CREATE TABLE dmk_stage_db.raw.openalex_sources (
    id NVARCHAR(100) PRIMARY KEY,
    display_name NVARCHAR(500),
    issn NVARCHAR(255),
    is_oa BIT,
    host_organization NVARCHAR(500),
    works_count INT,
    created_date smalldatetime DEFAULT GETDATE()
);

CREATE TABLE dmk_stage_db.raw.openalex_institutions (
    id NVARCHAR(100) PRIMARY KEY,
    display_name NVARCHAR(500),
    country_code NVARCHAR(10),
    type NVARCHAR(100),
    ror NVARCHAR(255),
    works_count INT,
    created_date smalldatetime DEFAULT GETDATE()
);
