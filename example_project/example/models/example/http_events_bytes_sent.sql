{{ config(primary_key=["method"]) }}

SELECT CAST(envoy['method'] AS STRING)       AS `method`,
       SUM(CAST(envoy['bytes_sent'] AS INT)) AS `total_bytes_sent`
FROM (
         -- Match and parse Envoy records in the value field of the envoy_raw stream.
         -- grok() produces a map<field name, value> we call envoy.
         SELECT grok(
                        `value`,
                        '\[%{TIMESTAMP_ISO8601:timestamp}\] "%{DATA:method} %{DATA:original_path} %{DATA:protocol}" %{DATA:response_code} %{DATA:response_flags} %{NUMBER:bytes_rcvd} %{NUMBER:bytes_sent} %{NUMBER:duration} %{DATA:upstream_svc_time} "%{DATA:x_forwarded_for}" "%{DATA:useragent}" "%{DATA:request_id}" "%{DATA:authority}" "%{DATA:upstream_host}"'
                    ) AS envoy
         FROM envoy_raw)
GROUP BY envoy['method']
