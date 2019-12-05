# dynamodbdump: DynamoDB backups made easier

[![GoDoc](https://godoc.org/github.com/VEVO/dynamodbdump?status.svg)](https://godoc.org/github.com/VEVO/dynamodbdump)
[![Test Status](https://github.com/VEVO/dynamodbdump/workflows/tests/badge.svg)](https://github.com/VEVO/dynamodbdump/actions?query=workflow%3Atests)
[![Go Report Card](https://goreportcard.com/badge/github.com/VEVO/dynamodbdump)](https://goreportcard.com/report/github.com/VEVO/dynamodbdump)

## Table of Contents

  * [What is it?](#what-is-it)
  * [Why creating this tool?](#why-creating-this-tool)
  * [How to use it?](#how-to-use-it)
    * [With the command-line](#with-the-command-line)
    * [Inside docker](#inside-docker)
    * [Inside a Kubernetes job](#inside-a-kubernetes-job)
  * [Contributing to the project](#contributing-to-the-project)

## What is it?

This tool performs a backup of a given DynamoDB table and pushes it to a given folder in s3
in a format compatible with the AWS datapipeline functionality.

It is also capable of restoring a backup from s3 to a given table both from
this tool or from a backup generated using the datapipeline functionality.

## Why create this tool?

Using the AWS DataPipelines to backup DynamoDB tables spawns EMR clusters which
can take some time, and for small tables it will cost you 20min of EMR runs for
just a few seconds of backup time, which makes no sense.

This tool can be run in a command-line, in a docker container and ending up on a
Kubernetes cronjob very easily, allowing you to leverage your existing
architecture without additional costs.

## How to use it?

### With the command-line

To build dynamodbdump, you can use the `make build` command or manually get the
dependencies (using `glide` or `go get`) and then use `go build` to build.

Then you can use the `dynamodbdump` binary you just built to start a backup.

Example:
```
make build
./dynamodbdump -action backup -dynamo-table my-table -wait-ms 2000  -batch-size 1000 -s3-bucket my-dynamo-backup-bucket -s3-folder "backups/my-table" -s3-date-folder
```

Note: the command-line options are available via the `-h` argument. Example:
```
$ ./dynamodbdump -h
Usage of ./dynamodbdump:
  -action string
        Action to perform. Only accept 'backup' or 'restore'. Environment variable: ACTION (default "backup")
  -batch-size int
        Max number of records to read from the dynamo table at once or to write in case of a restore. Environment variable: BATCH_SIZE (default 1000)
  -dynamo-table string
        Name of the Dynamo table to backup from or to restore in. Environment variable: DYNAMO_TABLE
  -restore-append
        Appends the rows to a non-empty table when restoring instead of aborting. Environment variable: RESTORE_APPEND
  -s3-bucket string
        Name of the s3 bucket where to put the backup or where to restore from. Environment variable: S3_BUCKET
  -s3-date-folder
        Adds an autogenenated suffix folder named using the UTC date in the format YYYY-mm-dd-HH24-MI-SS to the provided S3 folder. Environment variable: S3_DATE_FOLDER
  -s3-folder string
        Path inside the s3 bucket where to put or grab (for restore) the backup. Environment variable: S3_FOLDER
  -wait-ms int
        Number of milliseconds to wait between batches. If a ProvisionedThroughputExceededException is encountered, the script will wait twice that amount of time before retrying. Environment variable: WAIT_MS (default 100)
```


### Inside docker

Each time a merge is done to the master branch, a docker image is built with the
latest tag. Each time a tag is pushed in the git repository, an image with the
corresponding tag is built on
[Docker Hub](https://hub.docker.com/r/vevo/dynamodbdump/) or
[Docker cloud](https://cloud.docker.com/app/vevo/repository/docker/vevo/dynamodbdump/general)
if you prefer.

To pull the image, you'll need docker installed and then just pull the image as
usual:
```
docker pull vevo/dynamodbdump:latest
```

Then, assign the environment variables you want to use.
If you are doing that locally you will need to provide AWS credentials using the
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` or, as this example shows, share
your `.aws` configuration folder and (optionnally) provide a `AWS_PROFILE`
environment variable.

For example:
```
docker run -e ACTION=backup \
           -e AWS_REGION=us-east-1 \
           -e BATCH_SIZE=100 \
           -e DYNAMO_TABLE='mytable' \
           -e S3_BUCKET='bucket-for-data-dumps' \
           -e S3_DATE_FOLDER=1 \
           -e S3_FOLDER='dynamodb/mytable' \
           -e WAIT_MS=500 \
           -e AWS_PROFILE=dev \
           -v $HOME/.aws:/root/.aws
           -t vevo/dynamodbdump:latest
```

Note that if you are inside AWS and have properly setup IAM roles you will not
need to specify credentials.

### Inside a Kubernetes job

In our setup we use IAM roles to get access to the DynamoDB tables and S3
buckets so we don't need to set AWS credentials.

We use dynamodbdump to run as cronjobs in all our kubernetes clusters that have
applications that use DynamoDB tables. That use case is really what the tool was
designed for.

An example of kubernetes configuration for such a cronjob can be found in
[resources/kubernetes-cronjob.yaml](https://github.com/VEVO/dynamodbdump/blob/master/resources/kubernetes-cronjob.yaml)

In this example we do a backup of the `mytable` table every night at 2AM into
`s3://bucket-for-data-dumps/dynamodb/mytable/${TIMESTAMP}` with `${TIMESTAMP}`
being the date at which the backup starts in the following format:
`YYYY-mm-dd-HH24-MI-SS`.

The backup will process by batch of 100 records and wait 500ms between each
batch. If an exception is encountered in the mean time, it will wait twice that
time before retrying if the exception is a `ProvisionedThroughputExceededException`.

Note that in the example we max the memory to 512M but most of the time 256M are sufficient.

## Contributing to the project

Anybody is more than welcome to create PR if you want to contribute to the
project. A minimal testing and explanations about the problem will be asked but
that's for sanity purposes.

We're friendly people, we won't bite if the code is not done the way we like! :)

If you don't have a lot of ideas but still want to contribute, we maintain a
list of ideas we want to explore in the
[TODO.md](https://github.com/VEVO/dynamodbdump/blob/master/TODO.md), you can
start here!

