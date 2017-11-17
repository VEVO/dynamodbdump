# dynamodbdump: DynamoDB backups made easier

## What is it?

This tool performs a backup of a given DynamoDB table and pushes it to a given folder in s3
in a format compatible with the AWS datapipeline functionality.

It is also capable of restoring a backup from s3 to a given table both from
this tool or from a backup generated using the datapipeline functionality.

Example:
```
make build
./dynamodbdump -action backup -dynamo-table my-table -wait-ms 2000  -batch-size 1000 -s3-bucket my-dynamo-backup-bucket -s3-folder "backups/my-table" -s3-date-folder
```

## Why not using the AWS Datapipelines?

Using the AWS DataPipelines to backup DynamoDB tables spawns EMR clusters which
can take some time, and for small tables it will cost you 20min of EMR runs for
just a few seconds of backup time, which makes no sense.

This tool can be run in a command-line, in a docker container and ending up on a
Kubernetes cronjob very easily, allowing you to leverage your existing
architecture without additional costs.
