# SQL-Data-Warehouse-Project-2
Building a modern data warehouse with PostgreSQL, including ETL processes, data modeling and analytics.

## 🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers: 

1. **Bronze Layer**: Stores raw data from the source systems. Data is ingested from CSV Files into PostgreSQL Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business ready data modeled into a star schema required for reporting and analytics.

   **A staging layer was implemented for incremental storing of raw data before appending and loaded back to the target system (bronze layer).**

## 📖 Project Overview
This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

## 🛠️ Technical Implementation
- **Database:** PostgreSQL (RDBMS)
- **Tools Used:**
  - Datasets: csv files
  - VS Code for SQL development
  - Power BI for dashboard visualization
  - GitHub for version control
  - DrawIO: Design data architecture, models, flows, and diagrams.
 
## 🚀 Project Requirements
Building the Data Warehouse (Data Engineering)
**Objective**
Develop a modern data warehouse using PostgreSQL to consolidate sales data, enabling analytical reporting and informed decision making.

**Specifications**
* **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
* **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
* **Integration**: Combine both sources into a single, user friendly data model designed for analytical queries.
* **Scope**: Focus on the latest dataset only; historization of data is not required.
* **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

## BI: Analytics & Reporting (Data Analysis)
**Objective**
Develop SQL based analytics to deliver detailed insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**
These insights empower stakeholders with key business metrics, enabling strategic decision-making.

## 📂 Repository Structure
```markdown
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
└── .gitignore                          # Files and directories to be ignored by Git
```

## 🌟 About Me
I'm Chibuzo Nwankwo, I'm a data & business intelligence analyst and data engineer who loves to meet new people to share ideas, collaborate and learn new things.

GitHub: nchibuzo74

Email: nwankwochibuzosamuel24@gmail.com

LinkedIn: https://www.linkedin.com/in/chibuzo-nwankwo-baa756133
