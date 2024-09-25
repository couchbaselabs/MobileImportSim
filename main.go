// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"mobileImportSim/base"
	"mobileImportSim/dcp"
	"mobileImportSim/utils"

	"github.com/couchbase/gocbcore/v10"
	xdcrLog "github.com/couchbase/goxdcr/v8/log"
)

var options struct {
	numberOfDcpClients          uint64
	numberOfWorkersPerDcpClient uint64
	numberOfBins                uint64
	dcpHandlerChanSize          uint64
	bucketOpTimeout             uint64
	bucketBufferCapacity        int
	hostAddr, bucketname        string
	username, password          string
	debugMode                   uint64 // 0 - default=off, 1 - xdcrDebugLogging, 2 - xdcrDebugLogging + SDK verbose logging
}

func argParse() {
	flag.StringVar(&options.hostAddr, "hostAddr", "127.0.0.1:8091",
		"HostAddress for cluster")
	flag.StringVar(&options.username, "username", "",
		"Username for source cluster")
	flag.StringVar(&options.password, "password", "",
		"Password for cluster")
	flag.StringVar(&options.bucketname, "bucketname", "",
		"Bucket of cluster")
	flag.Uint64Var(&options.dcpHandlerChanSize, "dcpHandlerChanSize", base.DcpHandlerChanSize,
		"size of dcp handler channel")
	flag.Uint64Var(&options.bucketOpTimeout, "bucketOpTimeout", base.BucketOpTimeout,
		" timeout for bucket for stats collection, in seconds")
	flag.Uint64Var(&options.numberOfBins, "numberOfBins", 5,
		"number of buckets per vbucket")
	flag.IntVar(&options.bucketBufferCapacity, "bucketBufferCapacity", base.BucketBufferCapacity,
		"  number of items kept in memory per binary buffer bucket")
	flag.Uint64Var(&options.numberOfDcpClients, "numberOfSourceDcpClients", 1,
		"number of source dcp clients")
	flag.Uint64Var(&options.numberOfWorkersPerDcpClient, "numberOfWorkersPerSourceDcpClient", 64,
		"number of workers for each source dcp client")
	flag.Uint64Var(&options.debugMode, "debugMode", 0,
		"Turn on xdcr debug logging and SDK verbose logging. 0 is for off (default). 1 is for xdcr debug logging. 2 is for xdcr debug logging + SDK versbose logging")

	flag.Parse()
}

type mobileImportSimStateType int

const (
	StateInitial    mobileImportSimStateType = iota
	StateDcpStarted mobileImportSimStateType = iota
	StateFinal      mobileImportSimStateType = iota
)

type mobileImportSimState struct {
	state mobileImportSimStateType
	mtx   sync.Mutex
}

type mobileImportSim struct {
	dcpDriver *dcp.DcpDriver
	logger    *xdcrLog.CommonLogger
	curState  mobileImportSimState
}

func NewMobileImportSim(debugMode uint64) (*mobileImportSim, error) {
	var err error
	ctx := xdcrLog.DefaultLoggerContext

	if debugMode == 1 {
		ctx.Log_level = xdcrLog.LogLevelDebug
	} else if debugMode == 2 {
		if debugMode == 1 {
			gocbcore.SetLogger(gocbcore.VerboseStdioLogger())
			ctx.Log_level = xdcrLog.LogLevelDebug
		}
	}

	importSim := &mobileImportSim{
		logger: xdcrLog.NewLogger("mobileImportSim", ctx),
	}

	// Capture any Ctrl+C for continuing to next steps or cleanup
	go importSim.monitorInterruptSignal()

	return importSim, err
}

func main() {
	argParse()

	importSim, err := NewMobileImportSim(options.debugMode)
	if err != nil {
		importSim.logger.Errorf("Error creating mobileImportSim: %v", err)
		os.Exit(1)
	}

	importSim.logger.Infof("mobileImportSim is run with options: %+v", options)

	err = importSim.start()
	if err != nil {
		importSim.logger.Errorf("Error quitting. err=%v", err)
		os.Exit(1)
	}
}

func (importSim *mobileImportSim) start() error {
	importSim.logger.Infof("Mobile Import Simulator started")
	defer importSim.logger.Infof("Mobile Import Simulator exiting")

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	importSim.logger.Infof("Starting dcp driver")
	importSim.dcpDriver = startDcpDriver(options.hostAddr, options.bucketname,
		options.username, options.password, options.numberOfDcpClients,
		options.numberOfWorkersPerDcpClient, options.numberOfBins, options.dcpHandlerChanSize,
		options.bucketOpTimeout, errChan, waitGroup, options.bucketBufferCapacity, importSim.logger)

	importSim.curState.mtx.Lock()
	importSim.curState.state = StateDcpStarted
	importSim.curState.mtx.Unlock()

	importSim.logger.Infof("Started dcp driver, will wait for manual interrupt (CTRL+C) to exit")

	err := importSim.waitForCompletion(importSim.dcpDriver, errChan, waitGroup)

	return err
}

func startDcpDriver(url, bucketName, username, password string, numberOfDcpClients, numberOfWorkersPerDcpClient, numberOfBins, dcpHandlerChanSize, bucketOpTimeout uint64, errChan chan error, waitGroup *sync.WaitGroup, bucketBufferCap int, logger *xdcrLog.CommonLogger) *dcp.DcpDriver {
	waitGroup.Add(1)

	dcpDriver := dcp.NewDcpDriver(url, bucketName, username, password, int(numberOfDcpClients), int(numberOfWorkersPerDcpClient), int(numberOfBins),
		int(dcpHandlerChanSize), time.Duration(bucketOpTimeout)*time.Second, errChan, waitGroup, bucketBufferCap, logger)

	// dcp driver startup may take some time. Do it asynchronously
	go startDcpDriverAysnc(dcpDriver, errChan, logger)
	return dcpDriver
}

func startDcpDriverAysnc(dcpDriver *dcp.DcpDriver, errChan chan error, logger *xdcrLog.CommonLogger) {
	err := dcpDriver.Start()
	if err != nil {
		logger.Errorf("Error starting dcp driver %v. err=%v", dcpDriver.Name, err)
		utils.AddToErrorChan(errChan, err)
	}
}

func (importSim *mobileImportSim) waitForCompletion(sourceDcpDriver *dcp.DcpDriver, errChan chan error, waitGroup *sync.WaitGroup) error {
	doneChan := make(chan bool, 1)
	go utils.WaitForWaitGroup(waitGroup, doneChan)

	select {
	case err := <-errChan:
		importSim.logger.Errorf("Stop diff generation due to error from dcp client %v", err)
		err1 := sourceDcpDriver.Stop()
		if err1 != nil {
			importSim.logger.Errorf("Error stopping source dcp client. err=%v", err1)
		}

		return err
	case <-doneChan:
		importSim.logger.Infof("Source cluster and target cluster have completed")
		return nil
	}
}

func (importSim *mobileImportSim) monitorInterruptSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for sig := range c {
		if sig.String() == "interrupt" {
			importSim.curState.mtx.Lock()
			switch importSim.curState.state {
			case StateInitial:
				os.Exit(0)
			case StateDcpStarted:
				fmt.Println("Received interrupt. Closing DCP drivers")
				importSim.dcpDriver.Stop()
				importSim.curState.state = StateFinal
			case StateFinal:
				os.Exit(0)
			}
			importSim.curState.mtx.Unlock()
		}
	}
}
