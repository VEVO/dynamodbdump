# TODO list

Few ideas that might be worth exploring:
* update `README.md` with a little more stuffs! (tested with KMS-encrypted tables etc)
* add the accesskey, secret, region, profile etc in the `README.md`
* kook to dockerhub public
* hook travis
* write tests for all functions and examples too
* add Kubernetes examples
* variable StorageClass for backup
* variable ServerSideEncryption for backup
* add possible expiration for s3 backup files
* add possible set of tags for s3 backup files
* add logic to wait more after a certain consumed capacity threshold
* switch logging to logrus
* add verbose mode
* source and target of backup/restore other than s3:
  * restore directly from a given table instead of a s3 backup (sync tables functionnality)
  * backup/restore with local files as source
* add the ability to zip the files (not compatible with datapipelines)
* add flag to truncate (delete all items, not very good for big tables) the table before restore
* add the ability to backup the schema to recreate the table later (not compatible with datapipelines)
* add flag to recreate table from schema before restore
* for the restore of backups created with -s3-date-folder restore the last available
* add a flag to force restore even if the `_SUCCESS` file is absent
* add a flag to force restore all in the folder if the `manifest.json` is absent (that would build an in-memory manifest with the files)
* add the ability to backup and restore to a local dynamo
* backup multiple tables (1 folder per tbl)
* add autodiscovery of tables based on tags (and a pool of workers to backup tables in parallel)
* review code files separation (by aws service or by tool functions (backup/restore/common)?
* add the possibility to export the data uncrypted in the case of kms tables
* region for s3 may be different from region for dynamo?
