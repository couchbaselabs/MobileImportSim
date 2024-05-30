package dcp

import (
	"crypto/x509"
	"fmt"
	"mobileImportSim/base"
	"mobileImportSim/gocbcoreUtils"
	"mobileImportSim/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gomemcached"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbaselabs/gojsonsm"
)

// implements StreamObserver
type DcpHandler struct {
	dcpClient                     *DcpClient
	agent                         *gocbcore.Agent
	index                         int
	vbList                        []uint16
	numberOfBins                  int
	dataChan                      chan *Mutation
	waitGrp                       sync.WaitGroup
	finChan                       chan bool
	incrementCounter              func()
	incrementSysOrUnsubbedCounter func()
	bufferCap                     int
}

func NewDcpHandler(dcpClient *DcpClient, index int, vbList []uint16, numberOfBins, dataChanSize int, incReceivedCounter, incSysOrUnsubbedEvtReceived func(), bufferCap int) (*DcpHandler, error) {
	if len(vbList) == 0 {
		return nil, fmt.Errorf("vbList is empty for handler %v", index)
	}

	agent := gocbcoreUtils.CreateSDKAgent(&gocbcore.AgentConfig{
		SeedConfig: gocbcore.SeedConfig{MemdAddrs: []string{dcpClient.dcpDriver.bucketConnStr}},
		BucketName: dcpClient.dcpDriver.bucketName,
		UserAgent:  fmt.Sprintf("mobileImportSim_%v/%v_%v", dcpClient.url, dcpClient.dcpDriver.bucketName, index),
		SecurityConfig: gocbcore.SecurityConfig{
			UseTLS: false,
			TLSRootCAProvider: func() *x509.CertPool {
				return nil
			},
			Auth: gocbcore.PasswordAuthProvider{
				Username: dcpClient.username,
				Password: dcpClient.password,
			},
			AuthMechanisms: base.ScramShaAuth,
		},
		KVConfig: gocbcore.KVConfig{
			ConnectTimeout: dcpClient.gocbcoreDcpFeed.SetupTimeout,
			MaxQueueSize:   base.DcpHandlerChanSize * 50, // Give SDK some breathing room
		},
		CompressionConfig: gocbcore.CompressionConfig{Enabled: true},
		HTTPConfig:        gocbcore.HTTPConfig{ConnectTimeout: dcpClient.gocbcoreDcpFeed.SetupTimeout},
		IoConfig:          gocbcore.IoConfig{UseCollections: true},
	}, dcpClient.dcpDriver.logger)
	return &DcpHandler{
		dcpClient:                     dcpClient,
		index:                         index,
		vbList:                        vbList,
		numberOfBins:                  numberOfBins,
		dataChan:                      make(chan *Mutation, dataChanSize),
		finChan:                       make(chan bool),
		incrementCounter:              incReceivedCounter,
		incrementSysOrUnsubbedCounter: incSysOrUnsubbedEvtReceived,
		bufferCap:                     bufferCap,
		agent:                         agent,
	}, nil
}

func (dh *DcpHandler) Start() error {
	dh.waitGrp.Add(1)
	go dh.processData()

	return nil
}

func (dh *DcpHandler) Stop() {
	close(dh.finChan)
	dh.agent.Close()
}

func (dh *DcpHandler) processData() {
	dh.dcpClient.dcpDriver.logger.Infof("%v DcpHandler %v processData starts...", dh.dcpClient.Name, dh.index)
	defer dh.dcpClient.dcpDriver.logger.Infof("%v DcpHandler %v processData exits...", dh.dcpClient.Name, dh.index)
	defer dh.waitGrp.Done()

	for {
		select {
		case <-dh.finChan:
			goto done
		case mut := <-dh.dataChan:
			dh.processMutation(mut)
		}
	}
done:
}

func (dh *DcpHandler) simulateMobileImport(mut *Mutation) {
	dh.dcpClient.dcpDriver.logger.Debugf("Simulating import, key=%s for mutation=%v", mut.Key, mut)
	errCnt, importCnt := mut.SimulateImport(dh.agent, dh.dcpClient.dcpDriver.logger, dh.dcpClient.dcpDriver.bucketUUID)
	atomic.AddUint64(&dh.dcpClient.dcpDriver.totalFatalErrors, uint64(errCnt))
	atomic.AddUint64(&dh.dcpClient.dcpDriver.totalNewImports, uint64(importCnt))
}

func (dh *DcpHandler) processMutation(mut *Mutation) {
	dh.incrementCounter()

	// Ignore system events
	// Ignore unsubscribed events - mutations/events from collections not subscribed during OpenStream
	// we only care about actual data
	if mut.IsSystemOrUnsubbedEvent() {
		dh.incrementSysOrUnsubbedCounter()
		return
	}

	dh.simulateMobileImport(mut)
}

func (dh *DcpHandler) writeToDataChan(mut *Mutation) {
	select {
	case dh.dataChan <- mut:
	// provides an alternative exit path when dh stops
	case <-dh.finChan:
	}
}

func (dh *DcpHandler) SnapshotMarker(snapshot gocbcore.DcpSnapshotMarker) {
}

func (dh *DcpHandler) Mutation(mutation gocbcore.DcpMutation) {
	dh.writeToDataChan(CreateMutation(mutation.VbID, mutation.Key, mutation.SeqNo, mutation.RevNo, mutation.Cas, mutation.Flags, mutation.Expiry, gomemcached.UPR_MUTATION, mutation.Value, mutation.Datatype, mutation.CollectionID))
}

func (dh *DcpHandler) Deletion(deletion gocbcore.DcpDeletion) {
	dh.writeToDataChan(CreateMutation(deletion.VbID, deletion.Key, deletion.SeqNo, deletion.RevNo, deletion.Cas, 0, 0, gomemcached.UPR_DELETION, deletion.Value, deletion.Datatype, deletion.CollectionID))
}

func (dh *DcpHandler) Expiration(expiration gocbcore.DcpExpiration) {
	dh.writeToDataChan(CreateMutation(expiration.VbID, expiration.Key, expiration.SeqNo, expiration.RevNo, expiration.Cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, expiration.CollectionID))
}

func (dh *DcpHandler) End(streamEnd gocbcore.DcpStreamEnd, err error) {
	dh.dcpClient.dcpDriver.handleVbucketCompletion(streamEnd.VbID, err, "dcp stream ended")
}

// want CreateCollection("github.com/couchbase/gocbcore/v10".DcpCollectionCreation)
func (dh *DcpHandler) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	dh.writeToDataChan(CreateMutation(creation.VbID, creation.Key, creation.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, creation.CollectionID))
}

func (dh *DcpHandler) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	dh.writeToDataChan(CreateMutation(deletion.VbID, nil, deletion.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, deletion.CollectionID))
}

func (dh *DcpHandler) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	// Don't care - not implemented anyway
}

func (dh *DcpHandler) CreateScope(creation gocbcore.DcpScopeCreation) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(creation.VbID, nil, creation.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, creation.ScopeID))
}

func (dh *DcpHandler) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(deletion.VbID, nil, deletion.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, deletion.ScopeID))
}

func (dh *DcpHandler) ModifyCollection(modify gocbcore.DcpCollectionModification) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(CreateMutation(modify.VbID, nil, modify.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, modify.CollectionID))
}

func (dh *DcpHandler) OSOSnapshot(oso gocbcore.DcpOSOSnapshot) {
	// Don't care
}

func (dh *DcpHandler) SeqNoAdvanced(seqnoAdv gocbcore.DcpSeqNoAdvanced) {
	// This is needed because the seqnos of mutations/events of collections to which the consumer is not subscribed during OpenStream() has to be recorded
	// Eventhough such mutations/events are not streamed by the producer
	// bySeqno stores the value of the current high seqno of the vbucket
	// collectionId parameter of CreateMutation() is insignificant
	dh.writeToDataChan(CreateMutation(seqnoAdv.VbID, nil, seqnoAdv.SeqNo, 0, 0, 0, 0, gomemcached.DCP_SEQNO_ADV, nil, 0, base.Uint32MaxVal))
}

type Mutation struct {
	Vbno              uint16
	Key               []byte
	Seqno             uint64
	RevId             uint64
	Cas               uint64
	Flags             uint32
	Expiry            uint32
	OpCode            gomemcached.CommandCode
	Value             []byte
	Datatype          uint8
	ColId             uint32
	ColFiltersMatched []uint8 // Given a ordered list of filters, this list contains indexes of the ordered list of filter that matched
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, value []byte, datatype uint8, collectionId uint32) *Mutation {
	return &Mutation{
		Vbno:     vbno,
		Key:      key,
		Seqno:    seqno,
		RevId:    revId,
		Cas:      cas,
		Flags:    flags,
		Expiry:   expiry,
		OpCode:   opCode,
		Value:    value,
		Datatype: datatype,
		ColId:    collectionId,
	}
}

func (m *Mutation) IsExpiration() bool {
	return m.OpCode == gomemcached.UPR_EXPIRATION
}

func (m *Mutation) IsDeletion() bool {
	return m.OpCode == gomemcached.UPR_DELETION
}

func (m *Mutation) IsMutation() bool {
	return m.OpCode == gomemcached.UPR_MUTATION
}

func (m *Mutation) IsSystemOrUnsubbedEvent() bool {
	return m.OpCode == gomemcached.DCP_SYSTEM_EVENT || m.OpCode == gomemcached.DCP_SEQNO_ADV
}

func (m *Mutation) SimulateImport(agent *gocbcore.Agent, logger *xdcrLog.CommonLogger, bucketUUID string) (int, int) {
	key := m.Key
	body := m.Value
	colID := m.ColId
	datatype := m.Datatype
	casIn := m.Cas
	var syncCasIn uint64
	var importCasIn uint64

	fatalErrors := 0
	importedCnt := 0

	// retry until every step is successful
	simImport := func() error {
		if datatype&xdcrBase.PROTOCOL_BINARY_DATATYPE_XATTR > 0 {
			it, err := xdcrBase.NewXattrIterator(body)
			if err != nil {
				logger.Errorf("For key %s, colId %v, error while initing xattr iterator, err=%v\n", key, colID, err)
				return err
			}

			var err1 error
			for it.HasNext() && err1 == nil {
				k, v, err := it.Next()
				if err != nil {
					logger.Errorf("For key %s, colId %v, error while getting next xattr, err=%v\n", key, colID, err)
					err1 = err
					return err
				}

				switch string(k) {
				case gocbcoreUtils.XATTR_MOU:
					newMou := make([]byte, len(v))
					removed := make(map[string][]byte)
					_, _, _, err = gojsonsm.MatchAndRemoveItemsFromJsonObject(v, []string{gocbcoreUtils.IMPORTCAS, gocbcoreUtils.PREVREV}, newMou, removed)
					if err != nil {
						err1 = err
						return err
					}

					importCas, ok1 := removed[gocbcoreUtils.IMPORTCAS]
					if ok1 && importCas != nil {
						importCasIn, err = xdcrBase.HexLittleEndianToUint64(importCas[1 : len(importCas)-1])
						if err != nil {
							logger.Errorf("For key %s, colId %v, error while parsing importCas value, err=%v\n", key, colID, err)
							err1 = err
							return err
						}
					}
					if casIn < importCasIn {
						logger.Errorf("For key %s, colId %v, FATAL error of cas < importCas for mutation, err=%v\n", key, colID, err)
						fatalErrors++
						err1 = err
						return err
					}
				case xdcrBase.XATTR_MOBILE:
					syncIt, err := xdcrBase.NewCCRXattrFieldIterator(v)
					if err != nil {
						logger.Errorf("For key %s, colId %v, error while initing sync xattr iterator, err=%v\n", key, colID, err)
						err1 = err
						return err
					}

					var err2 error
					for syncIt.HasNext() && err2 == nil {
						k1, v1, err := syncIt.Next()
						if err != nil {
							logger.Errorf("For key %s, colId %v, error while getting next syncIt xattr, err=%v\n", key, colID, err)
							err2 = err
							return err
						}
						switch string(k1) {
						case gocbcoreUtils.SIMCAS:
							syncCasIn, err = xdcrBase.HexLittleEndianToUint64(v1)
							if err != nil {
								logger.Errorf("For key %s, colId %v, error while parsing syncCasIn, err=%v\n", key, colID, err)
								err2 = err
								return err
							}
						}
					}

					if err2 != nil {
						err1 = err2
						return err1
					}
				}
			}
		}

		casNow, syncCasNow, revIdNow, importCasNow, pvNow, mvNow, oldPvLen, oldMvLen, srcNow, verNow, cvCasNow, err := gocbcoreUtils.GetDocAsOfNow(agent, key, colID)
		if err != nil {
			logger.Errorf("For key %s, colId %v, error while subdoc-get err=%v\n", key, colID, err)
			return err
		}

		logger.Debugf("For key %s, colId %v, casIn %v, importCasIn %v, syncCasIn %v: casNow %v, cvCasNow %v, syncCasNow %v, revIdNow %v, importCasNow %v", key, colID, casIn, importCasIn, syncCasIn, casNow, cvCasNow, syncCasNow, revIdNow, importCasNow)
		if casIn > syncCasNow {
			// only process the mutation, if it has not been processed before i.e if casIn > syncCasNow.

			// 1. casIn == importCasIn and casIn == syncIn						-> local import mutation - skip import processing.
			// 2. casIn == importCasIn and synCasIn exists and casIn > syncIn	-> non-local or replicated post-import mutation - update importCas, but not HLV (cvCas will not be equal to cas for a replicated post-import mutation, but importCas will be equal to cas).
			// 3. casIn > importCasIn											-> non-import mutation - update both importCas and HLV.

			if casIn > importCasIn {
				postImportCas, err := gocbcoreUtils.WriteImportMutation(agent, key, casIn, importCasIn, casNow, revIdNow, srcNow, verNow, pvNow, mvNow, oldPvLen, oldMvLen, colID, bucketUUID, casNow > cvCasNow)
				if err != nil {
					logger.Errorf("For key %s, colId %v, error while subdoc-set err=%v\n", key, colID, err)
					return err
				}
				logger.Debugf("For key %s, colId %v, casIn %v: non-import mutation: postImportCas %v", key, colID, casIn, postImportCas)
				importedCnt++
			} else if syncCasIn != 0 && casIn > syncCasIn {
				postImportCas, err := gocbcoreUtils.WriteImportMutation(agent, key, casIn, importCasIn, casNow, revIdNow, srcNow, verNow, pvNow, mvNow, oldPvLen, oldMvLen, colID, bucketUUID, false)
				if err != nil {
					logger.Errorf("For key %s, colId %v, error while subdoc-set err=%v\n", key, colID, err)
					return err
				}
				logger.Debugf("For key %s, colId %v, casIn %v: non-local post-import mutation: postImportCas %v", key, colID, casIn, postImportCas)
				importedCnt++
			}
		}
		return nil
	}
	operationName := fmt.Sprintf("simImport for docKey: %s", m.Key)
	opErr := utils.ExponentialBackoffExecutor(operationName, time.Duration(base.SimulateImportRetryInterval)*time.Second, base.SimulateImportMaxRetries,
		base.SimulateImportBackOffFactor, time.Duration(base.SimulateImportMaxBackOff)*time.Second, simImport, logger)
	if opErr != nil {
		logger.Errorf("Failed to Simulate import for mutation pertaining to doc after max Retries: %s", m.Key)
		return fatalErrors, importedCnt
	}
	return fatalErrors, importedCnt
}
