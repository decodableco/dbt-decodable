{{
    config(
        output_stream={
            "schema_v2": {
                "watermarks": [
                    {
                        "name": "min_timestamp",
                        "expression": "min_timestamp - interval '0.100' second"
                    }
                ],
                "constraints": {
                    "primary_key": ["resource_type", "audit_event_type"],
                }
            }
        },
    )
}}
select
    coalesce(resource_type, '__UNKNOWN__') as resource_type,
    coalesce(audit_event_type, '__UNKNOWN__') as audit_event_type,
    count(1) as count_observed,
    min(to_timestamp_ltz(`timestamp`, 3)) as min_timestamp,
    max(to_timestamp_ltz(`timestamp`, 3)) as max_timestamp
from (select * from _events)
group by resource_type, audit_event_type
