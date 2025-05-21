# Metadata-Driven ETL Framework for Complex Workflows in Apache Airflow

---

## About

This framework provides a metadata-driven orchestration layer integrated with Apache Airflow, enabling scalable, maintainable, and auditable ETL processes. It abstracts job definitions, task logic, source configurations, and execution states into centralized metadata tables, forming the "brain" of the system.

### Watch the Introduction Video on YouTube (5 min):

[![DMK Airflow ETL Video](https://img.youtube.com/vi/PLd0rHZdO3s/0.jpg)](https://www.youtube.com/watch?v=PLd0rHZdO3s)


## ✅ Main Features

- Contains a fully functional, stand-alone set of metadata tables, stored procedures, and python modules to support ETL processes.
- Extracts sample data from various data sources—flat files, relational databases (RDBMS), REST APIs(token-based auth, pagination, etc.), Kafka streaming, —with a strong emphasis on performance.
- Scheduled and orchestrated through Apache Airflow.

---

## Tech Stack

- **Apache Airflow**
- **Python**
- **MS SQL Server**
- **Postgres**
- **Kafka**
- **Docker** 

---

## Project Notes

- Microsoft SQL Server, installed on a separate machine, serves as the destination database and contains:
  1. Custom metadata tables (in a dedicated metadata schema)
  2. Both 'Stage' and 'Production' data tables
- Apache Airflow, running in Docker, is used for job scheduling and ETL orchestration.
- The `Extract` step is implemented via Python pandas, sqlalchemy and SQL Server stored procs.
- Both `Transform` and `Load` steps are handled via SQL stored procedures.
  
### Project Architecture: 

<img src="diagrams/Project-architecture.jpg" alt="Example" width="500" hight="300"/>

---

## 🧠 ETL-Metadata-framework Database Features

ETL-Metadata-framework is more than just a storage layer for job configs. It also includes Extract SQL queries, tables column mappings and more.

It provides support for 
- `Data Lineage` & Traceability (source systems, intermediate steps, and target destinations).
- Both informational and error db-logging tied to each job execution id.
- `Extract`-only or (`Transform` & `Load`)-only job structures.
- Storage of `Extract` sql queries as well as the tracking of the stored procs (aka 'scripts') used in `Transform` & `Load`.
- Full and Incremental `Extracts` based on incremental dates.
- `Large vs. Small Data` metadata switches resulting in the different approaches for performance (via python layer)
- Task-level activation on/off-switches without modifying the overall job
- Configurable cleanup for the temp structures (extract temp tables, flat files) via metadata on/off-switches


### ETL-Metadata-tables schema 

<img src="diagrams/metadata-db-schema.jpg" alt="Example" width="500" hight="300"/>

### Documentation:

[AirFlow-ETL-Presentation](https://github.com/klionsky123/dmk-airflow-etl/blob/main/diagrams/AirFlow-ETL-Presentation.pdf)

---

## Road Map

- Add Use cases for streaming data (Kafka), Parque files (AWS)
- Add support for PostgreSQL metadata store (currently, MS SQL Server only)



