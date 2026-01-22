
# Implementing Real-Time intelligence using MongoDB CDC Connector for Fabric Eventstream

This sample demonstrates an end-to-end real-time data ingestion and analytics pipeline using:

- **Microsoft Fabric Eventstream**
- **MongoDB CDC Connector for Eventstream**
- **Fabric Eventhouse (KQL database)**
- **Medallion Architecture (Bronze → Silver → Gold)**
- **Update Policies**
- **KQL Materialized Views**
- **Fabric Real-Time Dashboards**

The demo simulates a real-world **Insurance Claims Processing** scenario where updates in a MongoDB database flow through Fabric in real time to power fraud analytics, exposure tracking, and claims operations dashboards.

This repository includes:

- Full KQL scripts for Bronze, Silver, and Gold layers  
- Eventstream configuration guidance  
- A Python-based synthetic data generator for MongoDB  
- Architecture diagrams  
- A structured, repeatable demo flow  
- A fully documented step-by-step setup guide  

