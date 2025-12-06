// use the AWS SDK v3 client that is built into the Lambda Node.js 18+ runtime
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({});

const API_KEY = process.env.API_KEY;
const BUCKET_NAME = process.env.BUCKET_NAME;

export const handler = async (event) => {
  try {
    if (!API_KEY) {
      throw new Error("Missing API_KEY environment variable");
    }
    if (!BUCKET_NAME) {
      throw new Error("Missing BUCKET_NAME environment variable");
    }

    // to set the dat for last week and get the right format for the url
    const now = new Date();
    const endDate = new Date(now);
    const startDate = new Date(now);
    startDate.setDate(startDate.getDate() - 6);
    const startDateStr = startDate.toISOString().split("T")[0];
    const endDateStr = endDate.toISOString().split("T")[0];

    // url for the neo near earth objects
    const url = `https://api.nasa.gov/neo/rest/v1/feed?start_date=${startDateStr}&end_date=${endDateStr}&api_key=${API_KEY}`;

    const response = await fetch(url);
    if (!response.ok) {
      const text = await response.text();
      console.error("NASA error body:", text);
      throw new Error(`Response status: ${response.status}`);
    }

    const result = await response.json();

    // data clean and order to create csv data comma separated
    const dates = Object.keys(result.near_earth_objects);
    const rows = [];

    dates.forEach((date) => {
      const neos = result.near_earth_objects[date];

      neos.forEach((neo) => {
        const ca = neo.close_approach_data[0];
        const diameterKm = neo.estimated_diameter.kilometers;

        const row = {
          // time
          approach_date: ca.close_approach_date,
          epoch_date_close_approach: ca.epoch_date_close_approach,

          // ids / names
          neo_id: neo.id,
          neo_reference_id: neo.neo_reference_id,
          name: neo.name,
          nasa_jpl_url: neo.nasa_jpl_url,

          // size / brightness
          absolute_magnitude_h: neo.absolute_magnitude_h,
          estimated_diameter_min_km: diameterKm.estimated_diameter_min,
          estimated_diameter_max_km: diameterKm.estimated_diameter_max,

          // flags
          is_potentially_hazardous_asteroid:
            neo.is_potentially_hazardous_asteroid,
          is_sentry_object: neo.is_sentry_object,

          // velocity
          relative_velocity_km_s: ca.relative_velocity.kilometers_per_second,
          relative_velocity_km_h: ca.relative_velocity.kilometers_per_hour,

          // distance
          miss_distance_km: ca.miss_distance.kilometers,
          miss_distance_lunar: ca.miss_distance.lunar,

          // orbit
          orbiting_body: ca.orbiting_body,
        };

        rows.push(row);
      });
    });

    console.log("Total rows:", rows.length);

    if (rows.length === 0) {
      throw new Error("No NEO rows for this date range");
    }

    const csvHeaders = Object.keys(rows[0]).toString();
    const csvData = rows.map((o) => {
      return Object.values(o);
    });
    const csv = [csvHeaders, ...csvData].join("\n");

    // upload the data into S3
    const key = `neo/neo_${startDateStr}_to_${endDateStr}.csv`;

    await s3.send(
      new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        Body: csv,
        ContentType: "text/csv",
      })
    );

    console.log(`Uploaded to s3://${BUCKET_NAME}/${key}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "NEO CSV generated and uploaded to S3",
        bucket: BUCKET_NAME,
        key,
        rows: rows.length,
        start_date: startDateStr,
        end_date: endDateStr,
      }),
    };
  } catch (error) {
    console.error("Error in Lambda:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error generating or uploading CSV",
        error: error.message,
      }),
    };
  }
};
