📊 Northwind Business Intelligence Project
📌 Overview

This project implements a complete Business Intelligence (BI) pipeline using the Northwind database as a case study.
It covers the full BI lifecycle: ETL (Extract, Transform, Load), Data Warehouse modeling, OLAP analysis, and interactive dashboards.

The goal is to transform operational data into decision-support information through a structured Data Warehouse and advanced visual analytics.

🎯 Objectives

Extract data from SQL Server and Microsoft Access

Clean, transform, and integrate data using Python

Build a Data Warehouse (Star Schema)

Compute Key Performance Indicators (KPIs)

Perform OLAP multidimensional analysis

Create interactive dashboards using Plotly

Ensure data quality and validation

🗂️ Project Structure
Northwind_project_BI/
│
├── data/
│   ├── raw/           # Extracted raw CSV files
│   ├── staging/       # Cleaned and transformed data
│   └── warehouse/     # Final Data Warehouse tables (CSV / Parquet)
│
├── scripts/
│   ├── extract_data.py
│   ├── transform_data.py
│   ├── load_dwh.py
│   └── etl_main.py
│
├── notebooks/
│   └── dashboard.ipynb
│
├── figures/
│   └── dashboard.png
│
├── reports/
│   └── Rapport_BI_Northwind_ING3_Final.pdf
│
├── videos/
│   └── video.mp4
│
└── README.md

🔄 ETL Process
1️⃣ Extraction

Data extracted from:

SQL Server (Northwind)

Microsoft Access (Northwind 2012.accdb)

Stored as CSV files in data/raw/

2️⃣ Transformation

Column normalization

Duplicate removal

Null value handling

Key harmonization

Business metrics calculation (e.g. total_amount)

Creation of surrogate keys

3️⃣ Load

Data loaded into the Data Warehouse

Output formats:

CSV

Parquet

Automatic schema generation

🧱 Data Warehouse Model
⭐ Star Schema

Fact table: FactSales

Dimensions:

DimDate

DimClient

DimEmployee

The schema is illustrated in schema_etoile.png.

📈 Dashboards & Analytics
KPIs

Total revenue

Number of orders

Delivered vs non-delivered orders

Visualizations

Delivery status (Green / Red)

Delivery performance by employee

Geographic sales distribution

Heatmaps (Employee × Time, Client × Time)

OLAP 3D analysis:

X: Date (Year / Month)

Y: Client

Z: Employee

Color: Total sales

All dashboards are implemented using Plotly.

🔍 Notebooks Description
📘 exploration.ipynb

Preview raw tables

Basic statistics

Initial data quality checks

📘 modelling.ipynb

Star Schema explanation

Dimension and fact creation

Schema visualization

📘 dashboard.ipynb

KPI computation

Interactive charts

OLAP 3D visualization

Geographic and heatmap analysis

📘 verification.ipynb

Null values check

Duplicate detection

Raw vs Data Warehouse row comparison

✅ Data Validation

Primary key uniqueness

Missing values detection

Consistency between raw data and Data Warehouse

🛠️ Technologies Used

Python

Pandas

SQLAlchemy

PyODBC

Plotly

Jupyter Notebook

SQL Server

Microsoft Access

🚀 How to Run the Project

Install dependencies:

pip install pandas pyodbc sqlalchemy plotly pyarrow


Run the ETL pipeline:

python scripts/etl_main.py


Open notebooks:

jupyter notebook


Explore dashboards in dashboard.ipynb

🎓 Academic Context

Level: ING3

Module: Business Intelligence

Case Study: Northwind

Focus: Data Warehouse, OLAP, Decision Support Systems

🔮 Future Improvements

Add Product and Supplier dimensions

Implement Slowly Changing Dimensions (SCD)

Deploy dashboards as a web app (Streamlit / Dash)

Integrate predictive analytics (Machine Learning)

Build a real OLAP cube (SSAS)

👤 Author

Tadj Eddine BOUDERBA    222231244012
Computer Engineering – Cybersecurity
ING3 – Business Intelligence Project