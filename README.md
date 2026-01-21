# Fabric Real-Time Claims CDC Demo

End-to-end Microsoft Fabric Real-Time CDC demo with MongoDB Atlas, Eventstream, KQL Medallion (Bronze → Silver → Gold), MVs, and a Real-Time Dashboard.

## Steps
1) Load data into MongoDB (mongoimport).

2) MongoDB prerequisites: reader user, network allowlist, pre/post images enabled.

3) Configure Eventstream (MongoDB CDC) → Bronze table `claims_raw_tbl`.

4) Create Silver table, transform, update policy.

5) Create Gold materialized views and dashboard tiles.

6) Insert more data via mongosh or run synthetic-data/seed_claims.py.

