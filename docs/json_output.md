# JSON Output

pg-healthcheck produces a structured JSON document when run with
`--output json`. The document includes a summary of findings by
severity and a full list of all check results. The following sections
describe the schema and how to use it.

## Schema

The output document contains three top-level fields: `timestamp`,
`hostname`, `pg_version`, `mode`, `summary`, and `checks`.

The following example shows a complete response document:

```json
{
  "timestamp": "2026-04-23T10:30:00Z",
  "hostname": "db1:5432",
  "pg_version": "16.2",
  "mode": "single",
  "summary": {
    "ok": 170,
    "info": 8,
    "warn": 2,
    "critical": 0,
    "total": 180
  },
  "checks": [
    {
      "check_id": "G14-002",
      "group": "WAL Growth and Generation Rate",
      "severity": "WARN",
      "title": "WAL generation rate",
      "observed": "WAL rate: 67.3 MB/s (over 2.1s sample)",
      "recommended": "Identify top WAL-generating tables; look for bulk writes or FPI storms.",
      "detail": "",
      "doc_url": "https://www.postgresql.org/docs/current/wal-configuration.html",
      "node_name": ""
    }
  ]
}
```

## Field Reference

The following table describes each field in the output document:

| Field | Type | Description |
|---|---|---|
| `timestamp` | string | ISO 8601 timestamp of when the run started |
| `hostname` | string | `host:port` of the target database |
| `pg_version` | string | PostgreSQL server version string |
| `mode` | string | `single` or `cluster` |
| `summary.ok` | integer | Count of checks that returned OK |
| `summary.info` | integer | Count of checks that returned INFO |
| `summary.warn` | integer | Count of checks that returned WARN |
| `summary.critical` | integer | Count of checks that returned CRITICAL |
| `summary.total` | integer | Total number of checks that ran |

The following table describes each field in a finding object within
the `checks` array:

| Field | Type | Description |
|---|---|---|
| `check_id` | string | Unique check identifier, e.g. `G09-004` |
| `group` | string | Human-readable group name |
| `severity` | string | `OK`, `INFO`, `WARN`, or `CRITICAL` |
| `title` | string | Short description of what was checked |
| `observed` | string | What the check measured or found |
| `recommended` | string | Action to take (empty for OK findings) |
| `detail` | string | Additional context (empty if not applicable) |
| `doc_url` | string | Link to PostgreSQL documentation |
| `node_name` | string | Node label in cluster mode (empty in single mode) |

## Severity Values

The `severity` field uses one of four values:

- `OK` means the check passed and the metric is within safe bounds.
- `INFO` means the check passed but has an advisory observation, such
  as a configuration recommendation or an optional extension that is
  not installed.
- `WARN` means the metric has exceeded a warning threshold and the
  situation should be investigated.
- `CRITICAL` means the metric has exceeded a critical threshold or a
  condition was found that requires immediate attention.

## Integration Examples

To extract only failing checks, use `jq` to filter by severity:

```bash
./pg-healthcheck --output json \
  | jq '.checks[] | select(.severity == "WARN" or .severity == "CRITICAL")'
```

To get a simple pass or fail status from the summary:

```bash
./pg-healthcheck --output json | jq '.summary.critical == 0 and .summary.warn == 0'
```

To pipe results into a monitoring system that accepts newline-delimited
JSON, use `jq` to output one finding per line:

```bash
./pg-healthcheck --output json | jq -c '.checks[]'
```
