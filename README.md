# Tennis Analytics Dashboard (Data Engineering)

Welcome to the Tennis Analytics Dashboard project! This project aims to provide comprehensive insights into tennis analytics for the major championships of 2023.

This repository store the code for

- loading raw dataset
- data transformation

For data dashboard app repo, please visit [tennis-analytics-app](https://github.com/jhueilim96/tennis-analytics-app).

View dashboard live [here](https://ta-app.ashymeadow-e82ce265.southeastasia.azurecontainerapps.io)

### Contributors

- Data Engineer - _[JH](https://github.com/jhueilim96)_
- Tennis Data Analyst - _[Azura Aziz](https://github.com/azuraaziz)_

### Purpose

This project serves as a demonstration of the skillset

- Dimensional Data Modelling
- Data Transformation
- Data Warehousing using [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## Key Components

_Dimensional Modelling_: Utilizing **star schemas** to organize sports data into fact tables and dimension tables for effiecient retrieval.

_Data Transformation_: Employing **dbt** as the transformation framework, including data cleaning, aggregating, and enriching data.

_Data Warehouse_: Utilizing **DuckDB** as the local data warehouse for fast query performance on analytical workloads.

_Medallion Framework_: Following the **Databricks medallion framework** to organize data into multiple layers (raw, bronze, silver, and gold) based on its processing stage and quality.

## Requirements

To run the Tennis Analytics Data Warehouse locally, you'll need to have the following dependencies installed:

- Python 3.10
- dbt
- DuckDB
- Other necessary Python libraries (specified in requirements.txt)

You can install the required dependencies using [uv](https://pypi.org/project/uv/) (a new performant python package manager) :

```bash
pip install uv
uv venv -p "3.10"
source .venv/Scripts/activate
uv pip install -r requirements.txt
```

## Usage

The post-transformed database binary file is included as `ta.duckdb` for ease of usage.

To reload the database from raw datasets, run the following command:

```bash
cd ta_dbt
dbt run
```

To data linage and catalog, run the following command:

```bash
cd ta_dbt
dbt docs generate
dbt docs serve
```

This will start a local server, and you can access the data catalog through your web browser.

## Acknowledgements

- All Tennis ATP data are credited to
  - [ultimatetennisstatisic](https://www.ultimatetennisstatistics.com/)
  - [JeffSackmann](https://github.com/JeffSackmann/tennis_atp)

## Contact

For any inquiries or feedback regarding the Tennis Analytics Dashboard project, feel free to contact contributors. We'd love to hear from you!
