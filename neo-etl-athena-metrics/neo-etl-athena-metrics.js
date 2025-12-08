import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from "@aws-sdk/client-athena";

const BUCKET_NAME = process.env.BUCKET_NAME;
const DATA_BASE_NAME = process.env.DATA_BASE_NAME;

const s3 = new S3Client({});
const athena = new AthenaClient({});

const buildTransformDataQuery = (folderDate) => `
WITH filtered AS (
  SELECT *
  FROM neo_raw_data
  WHERE CAST(approach_date AS date)
        BETWEEN date_add('day', -6, date '${folderDate}')
            AND date '${folderDate}'
),
stats AS (
  SELECT
    COUNT_IF(orbiting_body = 'Earth') AS total_in_earth_orbit,
    COUNT_IF(orbiting_body = 'Earth' AND miss_distance_lunar < 5) AS under_5_lunar_close,
    COUNT_IF(is_potentially_hazardous_asteroid = true) AS dangerous_count,
    COUNT_IF(orbiting_body <> 'Earth') AS total_out_earth_orbit,
    AVG(
      CASE
        WHEN orbiting_body = 'Earth'
          THEN (estimated_diameter_min_km + estimated_diameter_max_km) / 2
        ELSE NULL
      END
    ) AS avg_size_earth_orbit_km
  FROM filtered
),
biggest AS (
  SELECT
    name AS biggest_earth_object_name,
    estimated_diameter_max_km AS biggest_earth_object_diameter_km
  FROM filtered
  WHERE orbiting_body = 'Earth'
  ORDER BY estimated_diameter_max_km DESC
  LIMIT 1
),
smallest AS (
  SELECT
    name AS smallest_earth_object_name,
    estimated_diameter_max_km AS smallest_earth_object_diameter_km
  FROM filtered
  WHERE orbiting_body = 'Earth'
  ORDER BY estimated_diameter_max_km ASC
  LIMIT 1
)
SELECT
  stats.total_in_earth_orbit,
  stats.under_5_lunar_close,
  stats.dangerous_count,
  stats.total_out_earth_orbit,
  stats.avg_size_earth_orbit_km,
  biggest.biggest_earth_object_name,
  biggest.biggest_earth_object_diameter_km,
  smallest.smallest_earth_object_name,
  smallest.smallest_earth_object_diameter_km
FROM stats
CROSS JOIN biggest
CROSS JOIN smallest
`;

// Run an Athena query: start, poll until finished, then return GetQueryResults
const runAthenaQuery = async (q) => {
  const startRes = await athena.send(
    new StartQueryExecutionCommand({
      QueryString: q,
      QueryExecutionContext: {
        Database: DATA_BASE_NAME,
      },
    })
  );

  const queryExecutionId = startRes.QueryExecutionId;
  console.log("Started Athena query:", queryExecutionId);

  while (true) {
    const execRes = await athena.send(
      new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
    );
    const state = execRes.QueryExecution.Status.State;
    console.log("Athena state:", state);

    if (state === "SUCCEEDED") {
      break;
    }

    if (state === "FAILED" || state === "CANCELLED") {
      const reason = execRes.QueryExecution.Status.StateChangeReason;
      throw new Error(`Athena query ${state}: ${reason || "no reason"}`);
    }

    // wait 1 second before next status check
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  const resultsRes = await athena.send(
    new GetQueryResultsCommand({ QueryExecutionId: queryExecutionId })
  );

  return resultsRes;
};

export const handler = async (event) => {
  try {
    if (!BUCKET_NAME) {
      throw new Error("Missing BUCKET_NAME environment variable");
    }
    if (!DATA_BASE_NAME) {
      throw new Error("Missing DATA_BASE_NAME environment variable");
    }

    const now = new Date();
    const folderDate = now.toISOString().split("T")[0];

    const transformDataQuery = buildTransformDataQuery(folderDate);
    console.log("Running metrics query:\n", transformDataQuery);

    const response = await runAthenaQuery(transformDataQuery);

    const rows = response.ResultSet.Rows || [];
    if (rows.length < 2) {
      throw new Error("Metrics query returned no data rows");
    }

    const headerRow = rows[0].Data;
    const dataRow = rows[1].Data;

    const headers = headerRow.map((cell) => cell.VarCharValue);
    const values = dataRow.map((cell) => cell.VarCharValue ?? "");

    const headerLine = headers.join(",");
    const valueLine = values.join(",");
    const csv = `${headerLine}\n${valueLine}`;

    console.log("Metrics CSV:\n", csv);

    const key = `neo/transformed/${folderDate}.csv`;

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        Body: csv,
        ContentType: "text/csv",
      })
    );

    console.log(`Summary uploaded to s3://${BUCKET_NAME}/${key}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "NEO summary generated and uploaded to S3",
        bucket: BUCKET_NAME,
        key,
      }),
    };
  } catch (err) {
    console.error("Error in summary Lambda:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error generating NEO summary",
        error: err.message,
      }),
    };
  }
};
