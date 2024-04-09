# MobileImportSim
Couchbase mobile import process simulator for XDCR/Mobile Coexistence testing.

## To run:
```
$ make clean   # if needed, to start a fresh build process
$ make deps
$ make
$ ./mobileImportSim -username <username> -password <password> -bucketname <bucketname> -hostAddr <couchbase-node-hostaddress>

Example on a cluster_run setup: 
$ ./mobileImportSim -username Administrator -password wewewe -bucketname B1 -hostAddr 127.0.0.1:9000
```

## All options:
```
$ ./mobileImportSim -help
Usage of ./mobileImportSim:
  -bucketBufferCapacity int
    	  number of items kept in memory per binary buffer bucket (default 100000)
  -bucketOpTimeout uint
    	 timeout for bucket for stats collection, in seconds (default 20)
  -bucketname string
    	Bucket of cluster
  -dcpHandlerChanSize uint
    	size of dcp handler channel (default 100000)
  -debugMode uint
    	Turn on xdcr debug logging and SDK verbose logging. 0 is for off (default). 1 is for xdcr debug logging. 2 is for xdcr debug logging + SDK versbose logging
  -hostAddr string
    	HostAddress for cluster (default "127.0.0.1:8091")
  -numberOfBins uint
    	number of buckets per vbucket (default 5)
  -numberOfSourceDcpClients uint
    	number of source dcp clients (default 1)
  -numberOfWorkersPerSourceDcpClient uint
    	number of workers for each source dcp client (default 64)
  -password string
    	Password for cluster
  -username string
    	Username for cluster
```

## TODO:
- Add support for pruning by getting the pruning window (right no no pruning is done)
- Move _importCas and _prevRev to _mou after MB-60897 is done