export const handler = async (event) => {
  console.log("Event:", JSON.stringify(event, null, 2));

  const record = event.Records[0];
  const Bucket = record.s3.bucket.name;
  const Key = record.s3.object.key;

  console.log("File path:", Key);

  // Return a simple JSON response with the file name
  return {
    statusCode: 200,
    body: JSON.stringify({ Key, Bucket }),
  };
};
