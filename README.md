## Project idea

This project is a small serverless pipeline that tracks Near-Earth Objects (NEOs) using the NASA NEO API.

Once a week it:
- Fetches NEO data for the last 7 days.
- Stores the cleaned data as CSV in S3.
- Uses Athena to calculate weekly summary metrics.
- Saves a one-row CSV summary in S3 for reporting or notifications.

---

## Lambda functions

### 1. Lambda – raw NEO ingest (ETL layer 1)

- Called on a schedule (EventBridge).
- Calls the NASA NEO feed API for the last 7 days.
- Flattens the JSON into tabular data.
- Writes a CSV file to S3 under a date-based prefix (`neo/<endDate>/...`).

### 2. Lambda – weekly metrics (ETL layer 2)

- Called on a schedule after the ingest Lambda.
- Queries the `neo_raw_data` Athena table, limited to the last 7 days.
- Computes counts and basic stats (Earth orbit, <5 lunar, hazardous, biggest/smallest, etc.).
- Writes a one-row summary CSV to S3 (`neo/transformed/<date>.csv`).
---
## Architecture diagram
<img width="1720" height="1060" alt="Blank diagram (1)" src="https://github.com/user-attachments/assets/52fc09af-05dc-4ec1-a47a-91c1ef0abda6" />

