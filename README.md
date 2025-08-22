# Uber Data Engineering Pipeline

## Overview
This project builds an end-to-end data engineering pipeline for Uber data using Google Cloud Platform (GCP), Mage AI, BigQuery, and Looker. It ingests raw CSV data from Google Cloud Storage (GCS), processes it through an ETL pipeline, stores and analyses in BigQuery, and visualizes insights in Looker. 

---

## Tech Stack
- Google Cloud Storage (GCS) – Raw data storage
- Mage AI – Orchestration and ETL
- Google Compute Engine – Mage VM hosting
- BigQuery – Data warehouse for analytics
- Looker – Business intelligence dashboards
- Lucidchart – Data modeling

---

## Workflow
```mermaid 
flowchart LR
    %% Nodes
    GCS["Raw Data<br/>Cloud Storage"]
    subgraph ETL["ETL"]
      direction TB
      M["MAGE"]
      VM["Mage VM<br/>Compute Engine"]
      VM --> M
    end
    BQ["Analytics<br/>BigQuery"]
    L["Looker"]

    %% Edges
    GCS --> M
    M --> BQ
    BQ --> L

    %% Styling (optional; GitHub supports most of this)
    classDef node fill:#fff,stroke:#c9d3e0,stroke-width:1px,color:#111;
    class GCS,ETL,BQ,L,M,VM node;
    style ETL stroke-dasharray:6 4,stroke:#c9d3e0,fill:#f8fbff,stroke-width:1px;
```
---

## Data Model
<img width="1480" height="1171" alt="Data Model" src="https://github.com/user-attachments/assets/21706dfb-5e73-47df-824d-4ac1bacce912" />

---

## SQL Analytics
BigQuery SQL script created to join the fact table with all relevant dimension tables  (datetime_dim, passenger_count_dim, trip_distance_dim, rate_code_dim, pickup_location_dim,  drop_location_dim, and payment_type_dim) to build an enriched analytics table.  
In addition, SQL analysis was performed on:
- Top 10 pickup locations based on the number of trips
- Total number of trips by passenger count
- Average fare amount by hour of the day
  
---

## Results / Analysis - Looker
Summary Metrics: Total Revenue: $1.6M, Total Records: 100K trips, Average Trip Distance: 3.0 miles, Average Fare Amount: $13.25, Average Tip Amount: $1.87

Revenue by Payment Type: Credit Card: Highest revenue share (~$1.25M), Cash: Second largest share, No Charge / Dispute: Minimal revenue

Revenue by Rate Code: Standard Rate: Dominant revenue source (~$1.5M), JFK & Newark rates: Moderate contribution, Negotiated Fare / Group Ride: Smaller share

Geographic Insights: Pick-up locations visualized on an interactive Looker map for trend analysis
<img width="900" height="1275" alt="Uber_Dashboard" src="https://github.com/user-attachments/assets/0796ed05-5817-488f-8640-be41bf6faa41" />

---

  
