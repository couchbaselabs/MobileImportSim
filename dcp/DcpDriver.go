package dcp

import (
	"fmt"
	"mobileImportSim/base"
	"mobileImportSim/utils"
	"sync"
	"sync/atomic"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrHLV "github.com/couchbase/goxdcr/hlv"
	xdcrLog "github.com/couchbase/goxdcr/log"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
)

type DcpDriver struct {
	Name               string
	url                string
	bucketName         string
	errChan            chan error
	waitGroup          *sync.WaitGroup
	childWaitGroup     *sync.WaitGroup
	numberOfClients    int
	numberOfWorkers    int
	numberOfBins       int
	dcpHandlerChanSize int
	clients            []*DcpClient
	utils              xdcrUtils.UtilsIface
	// Value = true if processing on the vb has been completed
	vbStateMap map[uint16]*VBStateWithLock
	// 0 - not started
	// 1 - started
	// 2 - stopped
	state          DriverState
	stateLock      sync.RWMutex
	finChan        chan bool
	bufferCapacity int
	kvVbMap        map[string][]uint16
	actorId        xdcrHLV.DocumentSourceId

	// various counters
	totalNumReceivedFromDCP                uint64
	totalSysOrUnsubbedEventReceivedFromDCP uint64
	totalFatalErrors                       uint64
	totalNewImports                        uint64

	username, password string
	logger             *xdcrLog.CommonLogger

	// In the form of couchbase://<Public-IP>:<kvPort> where public-ip should be input as url and kvPort is fetched from kvVbMap, if not found 11210 is used
	bucketConnStr string
}

type VBStateWithLock struct {
	vbState VBState
	lock    sync.RWMutex
}

type VBState int

const (
	VBStateNormal       VBState = iota
	VBStateCompleted    VBState = iota
	VBStateStreamClosed VBState = iota
)

type DriverState int

const (
	DriverStateNew     DriverState = iota
	DriverStateStarted DriverState = iota
	DriverStateStopped DriverState = iota
)

func NewDcpDriver(url, bucketName, username, password string, numberOfClients, numberOfWorkers, numberOfBins, dcpHandlerChanSize int, bucketOpTimeout time.Duration, errChan chan error, waitGroup *sync.WaitGroup, bufferCap int, logger *xdcrLog.CommonLogger) *DcpDriver {
	dcpDriver := &DcpDriver{
		Name:               fmt.Sprintf("%v/%v", url, bucketName),
		url:                url,
		bucketName:         bucketName,
		numberOfClients:    numberOfClients,
		numberOfWorkers:    numberOfWorkers,
		numberOfBins:       numberOfBins,
		dcpHandlerChanSize: dcpHandlerChanSize,
		errChan:            errChan,
		waitGroup:          waitGroup,
		clients:            make([]*DcpClient, numberOfClients),
		childWaitGroup:     &sync.WaitGroup{},
		vbStateMap:         make(map[uint16]*VBStateWithLock),
		state:              DriverStateNew,
		finChan:            make(chan bool),
		bufferCapacity:     bufferCap,
		username:           username,
		password:           password,
		utils:              xdcrUtils.NewUtilities(),
		logger:             logger,
	}

	var vbno uint16
	for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
		dcpDriver.vbStateMap[vbno] = &VBStateWithLock{
			vbState: VBStateNormal,
		}
	}

	err := dcpDriver.initializeKVVBMapAndUUIDs()
	if err != nil {
		errMsg := fmt.Sprintf("error initing kvVbMap and UUIDs, err=%v", err)
		dcpDriver.logger.Fatalf(errMsg)
		panic(errMsg)
	}

	dcpDriver.bucketConnStr = base.GetBucketConnStr(dcpDriver.kvVbMap, dcpDriver.url, dcpDriver.logger)
	dcpDriver.logger.Infof("Using bucketConnStr=%v", dcpDriver.bucketConnStr)

	return dcpDriver

}

func (d *DcpDriver) Start() error {
	d.initializeDcpClients()

	err := d.startDcpClients()
	if err != nil {
		d.logger.Errorf("%v error starting dcp clients. err=%v", d.Name, err)
		return err
	}

	d.setState(DriverStateStarted)

	go d.checkForCompletion()

	return nil
}

func (d *DcpDriver) checkForCompletion() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var numOfCompletedVb int
			var vbno uint16
			for vbno = 0; vbno < base.NumberOfVbuckets; vbno++ {
				vbState := d.getVbState(vbno)
				if vbState != VBStateNormal {
					numOfCompletedVb++
				}
			}
			if numOfCompletedVb == base.NumberOfVbuckets {
				d.logger.Infof("%v all vbuckets have completed for dcp driver", d.Name)
				d.Stop()
				return
			}
		case <-d.finChan:
			d.logger.Infof("%v Received close channel", d.Name)
			return
		}
	}
}

func (d *DcpDriver) Stop() error {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	if d.state == DriverStateStopped {
		d.logger.Infof("Skipping stop() because dcp driver is already stopped")
		return nil
	}

	d.logger.Infof("Dcp driver %v stopping after receiving %v mutations (%v system + unsubscribed events), %v new imports and %v fatal errors", d.Name,
		atomic.LoadUint64(&d.totalNumReceivedFromDCP), atomic.LoadUint64(&d.totalSysOrUnsubbedEventReceivedFromDCP), atomic.LoadUint64(&d.totalNewImports), atomic.LoadUint64(&d.totalFatalErrors))
	defer d.logger.Infof("Dcp driver %v stopped", d.Name)
	defer d.waitGroup.Done()

	close(d.finChan)

	for i, dcpClient := range d.clients {
		if dcpClient != nil {
			err := dcpClient.Stop()
			if err != nil {
				d.logger.Errorf("Error stopping %vth dcp client. err=%v", i, err)
			}
		}
	}

	d.childWaitGroup.Wait()

	d.state = DriverStateStopped

	return nil
}

func (d *DcpDriver) initializeDcpClients() {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	loadDistribution := utils.BalanceLoad(d.numberOfClients, base.NumberOfVbuckets)
	for i := 0; i < d.numberOfClients; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = uint16(j)
		}

		d.childWaitGroup.Add(1)
		dcpClient := NewDcpClient(d, i, vbList, d.childWaitGroup, d.bufferCapacity, d.username, d.password, d.url)
		d.clients[i] = dcpClient
	}
}

func (d *DcpDriver) startDcpClients() error {
	for i, dcpClient := range d.getDcpClients() {
		err := dcpClient.Start()
		if err != nil {
			d.logger.Errorf("%v error starting dcp client. err=%v", d.Name, err)
			return err
		}
		d.logger.Infof("%v started dcp client %v", d.Name, i)
	}
	return nil
}

func (d *DcpDriver) getDcpClients() []*DcpClient {
	d.stateLock.RLock()
	defer d.stateLock.RUnlock()

	clients := make([]*DcpClient, len(d.clients))
	copy(clients, d.clients)
	return clients
}

func (d *DcpDriver) getState() DriverState {
	d.stateLock.RLock()
	defer d.stateLock.RUnlock()
	return d.state
}

func (d *DcpDriver) setState(state DriverState) {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()
	d.state = state
}

func (d *DcpDriver) reportError(err error) {
	// avoid printing spurious errors if we are stopping
	if d.getState() != DriverStateStopped {
		d.logger.Errorf("%s dcp driver encountered error=%v", d.Name, err)
	}

	utils.AddToErrorChan(d.errChan, err)
}

func allowedCompletionError(err error) bool {
	switch err {
	case gocbcore.ErrDCPStreamClosed:
		return true
	default:
		return false
	}
}

func (d *DcpDriver) handleVbucketCompletion(vbno uint16, err error, reason string) {
	if err != nil && !allowedCompletionError(err) {
		wrappedErr := fmt.Errorf("%v Vbno %v vbucket completed with err %v - %v", d.Name, vbno, err, reason)
		d.reportError(wrappedErr)
		vbStateWithLock := d.vbStateMap[vbno]
		vbStateWithLock.lock.Lock()
		defer vbStateWithLock.lock.Unlock()
		if vbStateWithLock.vbState == VBStateNormal {
			vbStateWithLock.vbState = VBStateCompleted
		}
	}
}

func (d *DcpDriver) getVbState(vbno uint16) VBState {
	vbStateWithLock := d.vbStateMap[vbno]
	vbStateWithLock.lock.RLock()
	defer vbStateWithLock.lock.RUnlock()
	return vbStateWithLock.vbState
}

func (d *DcpDriver) setVbState(vbno uint16, vbState VBState) {
	vbStateWithLock := d.vbStateMap[vbno]
	vbStateWithLock.lock.Lock()
	defer vbStateWithLock.lock.Unlock()
	vbStateWithLock.vbState = vbState
}

func (d *DcpDriver) IncrementDocReceived() {
	atomic.AddUint64(&d.totalNumReceivedFromDCP, 1)
}

func (d *DcpDriver) IncrementSysOrUnsubbedEventReceived() {
	atomic.AddUint64(&d.totalSysOrUnsubbedEventReceivedFromDCP, 1)
}

func (dcpDriver *DcpDriver) initializeKVVBMapAndUUIDs() error {
	var kvVbMap map[string][]uint16

	_, _, bucketUUID, _, _, kvVbMap, err := dcpDriver.utils.BucketValidationInfo(dcpDriver.url, dcpDriver.bucketName, dcpDriver.username,
		dcpDriver.password, xdcrBase.HttpAuthMechPlain, []byte{}, false, []byte{}, []byte{}, dcpDriver.logger)
	if err != nil {
		dcpDriver.logger.Errorf("initializeKVVBMap error=%v", err)
		return err
	}

	clusterUUID, err := dcpDriver.utils.GetClusterUUID(dcpDriver.url, dcpDriver.username, dcpDriver.password, xdcrBase.HttpAuthMechPlain, []byte{}, false, []byte{}, []byte{}, dcpDriver.logger)
	if err != nil {
		dcpDriver.logger.Errorf("GetClusterUUID error=%v", err)
		return err
	}

	actorId, err := xdcrHLV.UUIDstoDocumentSource(bucketUUID, clusterUUID)
	if err != nil {
		dcpDriver.logger.Errorf("UUIDstoDocumentSource error=%v, bucketUUID=%s, clusterUUID=%s", err, bucketUUID, clusterUUID)
		return err
	}

	dcpDriver.logger.Infof("BucketUUID for bucket %s = %s; ClusterUUID for url %s = %s; ActorId generated = %s; KvVbMap fetched for %v = %v",
		dcpDriver.bucketName, bucketUUID, dcpDriver.url, clusterUUID, actorId, dcpDriver.Name, kvVbMap)

	dcpDriver.kvVbMap = kvVbMap
	dcpDriver.actorId = actorId

	return nil
}
