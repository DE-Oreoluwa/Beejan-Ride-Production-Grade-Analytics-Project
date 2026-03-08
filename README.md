# Beejan Ride Analytics Engineering Project (Airbyte + dbt Core + BigQuery)

## Architecture diagram
Raw → Staging → Intermediate → Mart

![Alt text](https://github.com/DE-Oreoluwa/Beejan-Ride-Production-Grade-Analytics-Project/blob/1a2b196feb09175d7fe70a2ee0b19b0f6f40af1a/Architectural%20Diagram.png)

## ERD
The mart layer follows a star schema design where `fact_trips` acts as the central fact table connected to other dimension tables for analytical queries.

![Alt text](https://github.com/DE-Oreoluwa/Beejan-Ride-Production-Grade-Analytics-Project/blob/bab61bd83cdb818256078e23d57b63893901d594/ERD.png)

## Data flow explanation 
The project used Airbyte to extract data from Postgres (Source) and loaded it in BigQuery (Destination) warehouse.
With the use of dbt Core, the transformation was done with three layers which are Staging, Intermediate and Mart layer.

### 1. Source Layer
Raw data is ingested from operational systems into the data warehouse. These source tables contain ride-sharing platform data such as trips, drivers, riders, and cities. At this stage the data is not yet standardized and may contain inconsistencies or incomplete records.

### 2. Staging Layer

The staging layer standardizes and prepares raw data for downstream transformations. Models in this layer perform basic cleaning operations such as:
- Renaming columns to consistent naming conventions
- Casting data types
- Removing duplicates
- Handling null values
The goal of this layer is to create a clean and reliable representation of the source data while preserving its original structure.

### 3. Intermediate Layer

The intermediate layer applies business logic and calculates derived metrics required for analytics. Transformations in this layer include:
- Trip duration calculations
- Revenue and fare computations
- Fraud or anomaly indicators
 Payment validation logic
These models enrich the staged data and prepare it for analytical modeling.

### 4. Mart Layer
The mart layer organizes the transformed data into a star schema optimized for analytical queries. The central fact table stores trip transactions, while dimension tables provide descriptive attributes.
Models in this layer include:
- `fact_trips` – stores trip-level metrics such as duration, revenue, and payment status
- `dim_drivers` – driver attributes and operational status
- `dim_riders` – rider information and engagement data
- `dim_cities` – geographic attributes for trip analysis
This structure enables efficient queries for business metrics such as revenue trends, payment failure rates, daily revenue by city, driver performance, fraud indication, churn tracking and so on.

### 5. Analytics and Insights
The mart tables serve as the foundation for analytics use cases including:
- Operational monitoring
- Revenue reporting
- Fraud detection
- Rider and driver behavior analysis
Analysts and stakeholders can query these tables directly or connect them to BI dashboards for reporting and visualization.

## Design Decisions
### Schema design
In addition to the layers in tranformations, a star schema of 1 fact table and 3 dimension tables was designed and it; 
- It simplifies analytical queries and performance
- It separates transactional metrics from descriptive attributes which means metrics remain centralized, dimensions can be reused across analyses, and query complexity is reduced

### Data Quality and Testing
Data tests were implemented to ensure reliability of the analytics tables. These include checks such as:
- Trip duration must be greater than zero
- Revenue values cannot be negative
- Completed trips must have successful payments
- Source freshness checks for raw trip data
These tests help detect data issues early and maintain trust in downstream analytics.

### Incremental Processing
Large transactional datasets such as trips can grow quickly over time. To support scalability, models such as `fact_trips` can be configured using incremental materialization so that only new records are processed instead of rebuilding the entire dataset. This approach improves performance and reduces warehouse compute costs.
It was initially used for `fact_trips` but later changed to table materialization because dbt gives access to use and run incremental once for free.

### Documentation and Governance
Model descriptions, column documentation, tags, and ownership metadata are included in the project. This allows dbt to automatically generate a documentation site that improves transparency and collaboration for data stakeholders.
And this is the link to the dbt docs documentation: http://localhost:8080/#!/overview/dbt_beejanproject 

### Tradeoffs
- The mart layer uses a star schema to simplify analytical queries and improve performance for aggregations and reporting. While this structure is well suited for analytics workloads, it introduces some data redundancy compared to a fully normalized schema. The decision prioritizes query simplicity and performance over strict normalization, which is common in data warehouse design.
- For large datasets such as trips, incremental processing can improve performance by only transforming newly added data instead of rebuilding entire tables. However, incremental models can add complexity when handling late-arriving records or historical updates. A full refresh may still be required occasionally to ensure data consistency.
- The project focuses on a simple and maintainable data model with a limited number of dimension tables. While more advanced modeling could include additional dimensions (such as a date dimension or deeper operational metrics), the current design prioritizes clarity and ease of use for analytical queries.

### Future Improvements
Data quality monitoring could also be expanded by implementing more comprehensive tests and automated alerts for issues such as delayed data ingestion or unexpected metric changes.
