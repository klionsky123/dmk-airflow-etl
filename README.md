<h1>Fully working prototype: ETL with Airflow</h1>

MS SQL server is a destination server. It contains: 
  1. ETL metadata table (metadata schema)
  2. stage & production data tables.
     
Airflow is used for scheduling & ETL processing.

Extract part is done via python modules. Both Transform & Load steps are implemented via stored procedures.
<br/>Project Architecture: 
<br/>
<img src="diagrams/Project-architecture.jpg" alt="Example" width="500" hight="300"/>
<br/><br/>Meatadata tables for ETL jobs. This is a 'brain' of the system.
<br/>Metadata tables contain job & job tasks definitions, clients/data sources configurations as well as logs.
<br/>
<img src="diagrams/metadata-db-schema.jpg" alt="Example" width="500" hight="300"/>

Covered ETL Use cases:
https://github.com/klionsky123/dmk_etl_dag2/blob/main/diagrams/Covered-ETL-Use-cases2.jpg

AirFlow graph:
https://github.com/klionsky123/dmk_etl_dag2/blob/main/diagrams/Airflow-graph.jpg
