package importSim

import (
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goxdcr/base"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrCrMeta "github.com/couchbase/goxdcr/crMeta"
	xdcrLog "github.com/couchbase/goxdcr/log"
)

const XATTR_SYNC string = "_sync.simCas"
const XATTR_PREVREV string = "_prevRev"

type SubdocSetResult struct {
	Cas uint64
	Err error
}

// return Cas post-import and error
func generateImportMutation(agent *gocbcore.Agent, key []byte, casNow, revIdNow uint64, colID uint32) (uint64, error) {
	signal := make(chan SubdocSetResult)

	ops := make([]gocbcore.SubDocOp, 0)
	// _importCas is macro expanded
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		Path:  xdcrBase.XATTR_IMPORTCAS,
		Value: []byte(xdcrBase.CAS_MACRO_EXPANSION),
	})

	// _sync = casNow = pre-import Cas
	syncBytes := []byte("\"" + string(xdcrBase.Uint64ToHexLittleEndian(casNow)) + "\"")
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
		Path:  XATTR_SYNC,
		Value: syncBytes,
	})

	// _vv.cvCas = casNow = pre-import Cas
	cvCasBytes := []byte("\"" + string(xdcrBase.Uint64ToHexLittleEndian(casNow)) + "\"")
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
		Path:  xdcrCrMeta.XATTR_CVCAS_PATH,
		Value: cvCasBytes,
	})

	// _prevRev = revIdNow = pre-import revId
	revIdBytes := []byte("\"" + strconv.Itoa(int(revIdNow)) + "\"")
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
		Path:  XATTR_PREVREV,
		Value: revIdBytes,
	})

	// // _prevRev = revIdNow = pre-import revId
	// // pvBytes := []byte("\"" + strconv.Itoa(int(revIdNow)) + "\"")
	// ops = append(ops, gocbcore.SubDocOp{
	// 	Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
	// 	Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
	// 	Path:  xdcrCrMeta.XATTR_PV_PATH + "." + agent.ClientID(),
	// 	Value: cvCasBytes,
	// })

	agent.MutateIn(
		gocbcore.MutateInOptions{
			Key:          key,
			Cas:          gocbcore.Cas(casNow),
			Ops:          ops,
			CollectionID: colID,
			Flags:        memd.SubdocDocFlagAccessDeleted,
		}, func(mir *gocbcore.MutateInResult, err error) {
			signal <- SubdocSetResult{
				Cas: uint64(mir.Cas),
				Err: err,
			}
		},
	)
	res := <-signal
	if res.Err != nil {
		return 0, res.Err
	}
	return res.Cas, res.Err
}

type SubdocGetResult struct {
	Err                         error
	Cas, Sync, RevId, ImportCas uint64
}

func getDocAsOfNow(agent *gocbcore.Agent, key []byte, colID uint32) (cas, sync, revID, importCas uint64, err error) {
	signal := make(chan SubdocGetResult)

	ops := make([]gocbcore.SubDocOp, 0)
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  xdcrBase.VXATTR_REVID,
	})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  XATTR_SYNC,
	})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrBase.XATTR_IMPORTCAS),
	})
	agent.LookupIn(
		gocbcore.LookupInOptions{
			Key:          key,
			Flags:        memd.SubdocDocFlagAccessDeleted,
			CollectionID: colID,
			Ops:          ops,
		},
		func(lir *gocbcore.LookupInResult, err error) {
			var err1 error
			var res SubdocGetResult

			// cas
			res.Cas = uint64(lir.Cas)
			if err != nil {
				signal <- SubdocGetResult{Err: err}
				return
			}

			// revID
			if lir.Ops[0].Err == nil && len(lir.Ops[0].Value) > 0 {
				revIDStr := string(lir.Ops[0].Value)
				if revid, err1 := strconv.ParseUint(revIDStr[1:len(revIDStr)-1], 10, 64); err1 == nil {
					res.RevId = uint64(revid)
				} else {
					signal <- SubdocGetResult{Err: err1}
					return
				}
			}

			// _sync
			if lir.Ops[1].Err == nil && len(lir.Ops[1].Value) > 0 {
				syncBytes := lir.Ops[1].Value
				sync, err1 = xdcrBase.HexLittleEndianToUint64(syncBytes[1 : len(syncBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.Sync = sync
			}

			// _importCas
			if lir.Ops[2].Err == nil && len(lir.Ops[2].Value) > 0 {
				importCasBytes := lir.Ops[2].Value
				importCas, err1 = xdcrBase.HexLittleEndianToUint64(importCasBytes[1 : len(importCasBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.ImportCas = importCas
			}
			signal <- res
		},
	)
	result := <-signal
	err = result.Err
	if err != nil {
		return
	}

	cas = result.Cas
	revID = result.RevId
	sync = result.Sync
	importCas = result.ImportCas
	return
}

func SimulateImportForDcpMutation(agent *gocbcore.Agent, key, body []byte, colID uint32, datatype uint8, casIn uint64, logger *xdcrLog.CommonLogger) int {
	fatalErrors := 0
	for {
		var importCasIn uint64
		var err1 error
		if datatype&xdcrBase.PROTOCOL_BINARY_DATATYPE_XATTR > 0 {
			it, err := xdcrBase.NewXattrIterator(body)
			if err != nil {
				logger.Errorf("For key %s, colId %v, error while initing xattr iterator, err=%v\n", err)
				continue
			}
			for it.HasNext() && err1 == nil {
				k, v, err := it.Next()
				if err != nil {
					logger.Errorf("For key %s, colId %v, error while getting next xattr, err=%v\n", err)
					err1 = err
					break
				}

				switch string(k) {
				case base.XATTR_IMPORTCAS:
					importCasIn, err = base.HexLittleEndianToUint64(v[1 : len(v)-1])
					if err != nil {
						logger.Errorf("For key %s, colId %v, error while parsing importCasIn, err=%v\n", err)
						err1 = err
					}

					if casIn < importCasIn {
						logger.Errorf("For key %s, colId %v, FATAL error of cas < importCas for mutation, err=%v\n", err)
						err1 = err
						fatalErrors++
					}
				}
			}
		}

		if err1 != nil {
			continue
		}

		casNow, syncNow, revIdNow, importCasNow, err := getDocAsOfNow(agent, key, colID)
		if err != nil {
			logger.Errorf("For key %s, colId %v, error while subdoc-get err=%v\n", err)
			continue
		}

		logger.Debugf("For key %s, colId %v, casIn %v, importCasIn %v: casNow %v, syncNow %v, revIdNow %v, importCasNow %v", key, colID, casIn, importCasIn, casNow, syncNow, revIdNow, importCasNow)
		if casIn > syncNow && casIn > importCasIn {
			// only process if the mutation has not been processed before AND if the mutation is not an import mutation
			postImportCas, err := generateImportMutation(agent, key, casNow, revIdNow, colID)
			if err != nil {
				logger.Errorf("For key %s, colId %v, error while subdoc-set err=%v\n", err)
				continue
			}
			logger.Debugf("For key %s, colId %v, casIn %v: postImportCas %v", key, colID, casIn, postImportCas)
		}
		break
	}
	return fatalErrors
}

func CreateSDKAgent(agentConfig *gocbcore.AgentConfig, logger *xdcrLog.CommonLogger) *gocbcore.Agent {
	agent, err := gocbcore.CreateAgent(agentConfig)
	if err != nil {
		logger.Errorf("CreateAgent err=%v", err)
	}

	signal := make(chan error)
	_, err = agent.WaitUntilReady(time.Now().Add(15*time.Second),
		gocbcore.WaitUntilReadyOptions{
			DesiredState: gocbcore.ClusterStateOnline,
			ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService},
		}, func(res *gocbcore.WaitUntilReadyResult, err error) {
			signal <- err
		})

	if err == nil {
		err = <-signal
		if err != nil {
			logger.Errorf("Waited 15 seconds for bucket to be ready, err=%v\n", err)
			return nil
		}
	}

	return agent
}
