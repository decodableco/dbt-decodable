/* Example following the quickstart guide at https://docs.decodable.co/docs/quickstart-guide */

{{ config(watermark="`timestamp` AS `timestamp` - INTERVAL '0.001' SECOND") }}

SELECT
  TO_TIMESTAMP(CAST(envoy['timestamp'] AS STRING), 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS `timestamp`,
  CAST(envoy['method']            AS STRING) AS `method`,
  CAST(envoy['original_path']     AS STRING) AS original_path,
  CAST(envoy['protocol']          AS STRING) AS protocol,
  CAST(envoy['response_code']     AS INT)    AS response_code,
  CAST(envoy['response_flags']    AS STRING) AS response_flags,
  CAST(envoy['bytes_rcvd']        AS INT)    AS bytes_rcvd,
  CAST(envoy['bytes_sent']        AS INT)    AS bytes_sent,
  CAST(envoy['duration']          AS INT)    AS duration,
  CAST(envoy['upstream_svc_time'] AS INT)    AS upstream_svc_time,
  CAST(envoy['x_forwarded_for']   AS STRING) AS x_forwarded_for,
  CAST(envoy['useragent']         AS STRING) AS useragent,
  CAST(envoy['request_id']        AS STRING) AS request_id,
  CAST(envoy['authority']         AS STRING) AS authority,
  CAST(envoy['upstream_host']     AS STRING) AS upstream_host
FROM (
    -- Match and parse Envoy records in the value field of the envoy_raw stream.
    -- grok() produces a map<field name, value> we call envoy.
    SELECT
      grok(
        `value`,
        '\[%{TIMESTAMP_ISO8601:timestamp}\] "%{DATA:method} %{DATA:original_path} %{DATA:protocol}" %{DATA:response_code} %{DATA:response_flags} %{NUMBER:bytes_rcvd} %{NUMBER:bytes_sent} %{NUMBER:duration} %{DATA:upstream_svc_time} "%{DATA:x_forwarded_for}" "%{DATA:useragent}" "%{DATA:request_id}" "%{DATA:authority}" "%{DATA:upstream_host}"'
      ) AS envoy
    FROM envoy_raw
)
