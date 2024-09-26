package base

import "errors"

const DcpHandlerChanSize = 100000
const BucketBufferCapacity = 100000

// default values for configurable parameters if not specified by user
const BucketOpTimeout uint64 = 20

// Diff tool by default allow users to enter "http://<addr>:<ns_serverPort>"
const HttpPrefix = "http://"

// const HttpsPrefix = "https://"
const CouchbasePrefix = "couchbase://"

var SetupTimeoutSeconds int = 10

const Uint32MaxVal uint32 = 1<<32 - 1

const SimulateImportMaxBackOff = 64
const SimulateImportBackOffFactor = 2
const SimulateImportRetryInterval = 2
const SimulateImportMaxRetries = 15

var ErrorNotImported error = errors.New("Mutation was not imported")
