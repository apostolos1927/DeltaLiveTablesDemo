
CREATE OR REFRESH STREAMING TABLE sql_first_step
AS 
SELECT * FROM cloud_files(
  "/mnt/..../**/**/**/**/*.avro", "avro"
)



CREATE OR REFRESH STREAMING TABLE sql_bronze
AS 
SELECT body.deviceID as deviceID, 
      body.rpm as deviceRPM, 
      body.angle as deviceAngle,
      body.humidity as humidity,
      body.windspeed as windspeed, 
      body.temperature as temperature FROM (
      SELECT from_json(body,'deviceID INT,rpm INT, angle INT,humidity INT,windspeed INT,temperature INT') as body FROM (
          SELECT CAST(body as STRING) as body
          FROM STREAM(LIVE.sql_first_step)))



CREATE OR REFRESH STREAMING TABLE sql_silver
(
  CONSTRAINT deviceAngle EXPECT (deviceAngle IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT temperature EXPECT (temperature > 0) ON VIOLATION FAIL UPDATE
)
AS 
SELECT CAST(deviceID AS INT) as deviceID,
       deviceAngle,
       deviceRPM,
       humidity,
       windspeed,
       temperature
      FROM STREAM(LIVE.sql_bronze)



CREATE OR REFRESH STREAMING TABLE sql_gold
AS 
SELECT deviceID,
       deviceAngle,
       deviceRPM,
       humidity,
       windspeed,
       temperature
      FROM STREAM(LIVE.sql_silver)
      WHERE deviceID >=10
