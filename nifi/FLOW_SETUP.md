# NiFi Flow Setup (Real CSV -> MinIO, no ConvertRecord)

## Flow

`ListFile_all -> FetchFile_all -> RouteOnAttribute_csv -> PutS3Object_flexi / PutS3Object_icc`

No Controller Service is required for this raw CSV ingest flow.

## 1) ListFile_all

- `Input Directory`: `/data/input`
- `Input Directory Location`: `Local`
- `Listing Strategy`: `Tracking Timestamps`
- `Recurse Subdirectories`: `true`
- `File Filter`: `^frt_(flexi|in_icc)_export_.*\.csv$`
- `Include File Attributes`: `true`
- `Minimum File Age`: `0 sec`
- `Minimum File Size`: `0 B`
- `Polling Interval`: `30 sec`

Relationship:
- `success` -> `FetchFile_all`

## 2) FetchFile_all

- `File to Fetch`: `${absolute.path}${filename}`
- `Completion Strategy`: `None`
- `Move Conflict Strategy`: `Rename`
- `Log level when file not found`: `WARN`
- `Log level when permission denied`: `WARN`

Relationships:
- `success` -> `RouteOnAttribute_csv`
- `not.found` -> auto-terminate (or error queue)
- `permission.denied` -> auto-terminate (or error queue)
- `failure` -> auto-terminate (or error queue)

## 3) RouteOnAttribute_csv

Routing strategy: `Route to Property name`

Dynamic properties:
- `is_flexi`: `${filename:startsWith('frt_flexi_export_')}`
- `is_icc`: `${filename:startsWith('frt_in_icc_export_')}`

Relationships:
- `is_flexi` -> `PutS3Object_flexi`
- `is_icc` -> `PutS3Object_icc`
- `unmatched` -> auto-terminate

## 4) PutS3Object_flexi

- `Bucket`: `datalake`
- `Object Key`: `raw/flexi/${filename}`
- `Endpoint Override URL`: `http://minio:9000`
- `Access Key ID`: `minioadmin`
- `Secret Access Key`: `12345678`
- `Credentials File`: empty
- `Signer Override`: `Default Signature`
- `Use Chunked Encoding`: `true`
- `Use Path Style Access`: `true`
- `Multipart Threshold`: `5 MB`
- `Multipart Part Size`: `64 MB`

Relationships:
- `success` -> auto-terminate
- `failure` -> auto-terminate (or error queue)

## 5) PutS3Object_icc

Same as `PutS3Object_flexi`, only change:
- `Object Key`: `raw/icc/${filename}`

## 6) Start Order

1. Start `PutS3Object_flexi`, `PutS3Object_icc`
2. Start `RouteOnAttribute_csv`
3. Start `FetchFile_all`
4. Start `ListFile_all`

## 7) Reprocess Same Files

If file is not listed again:
1. Stop `ListFile_all`
2. Right click `ListFile_all` -> `View state` -> `Clear state`
3. Start again

## 8) Validate

MinIO (`http://localhost:9001`) should contain:
- `datalake/raw/flexi/frt_flexi_export_*.csv`
- `datalake/raw/icc/frt_in_icc_export_*.csv`
