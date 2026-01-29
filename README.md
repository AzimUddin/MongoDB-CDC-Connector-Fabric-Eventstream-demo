
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


## Architecture

This demo implements a full end-to-end **Real-Time Medallion Architecture** using MongoDB Atlas CDC and Microsoft Fabric Real-Time Intelligence. The design enables high‑throughput event ingestion, low-latency transformations, and real‑time analytics dashboards.

### 🔶 High-Level Data Flow

**MongoDB Atlas (Change Streams) → Eventstream (CDC Connector) → Fabric Eventhouse (KQL Database) → Bronze → Silver → Gold → Real-Time Dashboard**

The core components of the pipeline are:

1. **MongoDB Atlas**
   - Hosts the operational “claims” collection
   - Emits *insert*, *update*, and *replace* events via **Change Streams**
   - Pre/Post Images enabled for full CDC state capture

2. **Fabric Eventstream**
   - Uses the **MongoDB CDC connector**
   - Streams documents from MongoDB in real time
   - Provides transformations, data preview, and runtime diagnostics
   - Publishes into Fabric Eventhouse

3. **Fabric Eventhouse (KQL DB) – Bronze Layer**
   - Ingests raw CDC envelopes
   - Stores full Debezium-style “before/after” payloads
   - Append‑only table: `claims_raw_tbl`

4. **Silver Layer – Clean & Enriched**
   - Uses an **Update Policy** on Bronze to trigger the transform
   - Extracts record-level fields
   - Normalizes ISO timestamps
   - Produces clean schema in `claims_silver_tbl`

5. **Gold Layer – Business Aggregates**
   - Three Materialized Views:
     - **High Fraud Events (1‑minute)**
     - **Exposure by Region (15‑minute window)**
     - **Claims Closed per Hour**
   - Each MV automatically updates as Silver receives new events

6. **Fabric Real-Time Dashboard**
   - Reads materialized views directly
   - Auto‑refreshing tiles show real‑time trends:
     - Fraud spikes
     - Financial exposure
     - Operational efficiency

---

### 🔷 Architecture Diagram (Fabric Real-Time Intelligence Pipeline)

![Real-Time Intelligence Pipeline](./images/fabric-rti-architecture.png)

---

### 🔷 Medallion Architecture (Bronze → Silver → Gold)

This demo implements the **Real-Time Medallion pattern** using Eventhouse + Update Policies.

- **Bronze** — Raw CDC documents (full payload, no cleanup)  
- **Silver** — Cleaned, typed, normalized events  
- **Gold** — Aggregations and business KPIs (materialized views)  

Each layer is automated using **update policies**, keeping the system continuous and low-latency.

### Medallion Architecture (Bronze → Silver → Gold)
![Medallion Architecture](./images/fabric-medallion-rti.png)

---

