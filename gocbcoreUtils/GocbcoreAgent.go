package gocbcoreUtils

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goxdcr/base"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrCrMeta "github.com/couchbase/goxdcr/crMeta"
	"github.com/couchbase/goxdcr/hlv"
	xdcrHLV "github.com/couchbase/goxdcr/hlv"
	xdcrLog "github.com/couchbase/goxdcr/log"
)

const SIMCAS string = "simCas"
const PREVREV string = "pRev"
const IMPORTCAS string = "importCAS"
const XATTR_SYNC string = xdcrBase.XATTR_MOBILE + "." + SIMCAS
const XATTR_MOU string = "_mou"
const XATTR_PREVREV string = XATTR_MOU + "." + PREVREV
const XATTR_IMPORTCAS string = XATTR_MOU + "." + IMPORTCAS

type SubdocSetResult struct {
	Cas uint64
	Err error
}

// return Cas post-import and error, if any
func WriteImportMutation(agent *gocbcore.Agent, key []byte, importCasIn, casNow, revIdNow uint64, srcNow xdcrHLV.DocumentSourceId, verNow uint64, pvNow, mvNow xdcrHLV.VersionsMap, oldPvLen, oldMvLen uint64, colID uint32, bucketUUID string, updateHLV bool) (uint64, error) {
	signal := make(chan SubdocSetResult)
	// roll over mv to pv OR cv to pv, if needed
	src := xdcrHLV.DocumentSourceId(bucketUUID)
	pv := pvNow
	if len(mvNow) > 0 {
		// Add mv to pv, no need to add cv to history because it represents a merge event
		for k, v := range mvNow {
			pv[k] = v
		}
	} else if len(srcNow) > 0 {
		// Add cv to pv only if mv does not exist.
		// When there is no mv, cv represents a mutation and needs to be added to version history
		if len(pv) == 0 {
			pv = make(xdcrHLV.VersionsMap)
		}
		pv[srcNow] = verNow

	}
	// Make sure the cv is not repeated in pv
	delete(pv, src)

	pvMaxLen := oldPvLen + oldMvLen + uint64(len(srcNow)) +
		2 /* quotes for srcNow */ + 16 /* ver in hex */ + 2 /* 0x */ +
		2 /* quotes for verNow */ + 2 /* { and } */ + 1 /* : */
	pvBytes := make([]byte, pvMaxLen)
	pos := 0
	// 0 indicates no pruning (for now). TODO: Use pruning window
	pruneFunc := xdcrBase.GetHLVPruneFunction(casNow, 0)
	pos, _ = xdcrCrMeta.VersionMapToDeltasBytes(pv, pvBytes, pos, &pruneFunc)
	pvBytes = pvBytes[:pos]

	casNowBytes := []byte("\"" + string(xdcrBase.Uint64ToHexLittleEndian(casNow)) + "\"")

	ops := make([]gocbcore.SubDocOp, 0)

	// _mou.importCas = macro expanded
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		Path:  XATTR_IMPORTCAS,
		Value: []byte(xdcrBase.CAS_MACRO_EXPANSION),
	})
	// If this document is not imported before i.e. importCas=0 we should stamp _mou.pRev
	// If this document was imported before but the HLV is outdated due to a local mutation at the cluster then _mou.pRev should be updated
	if importCasIn == 0 || updateHLV {
		// _mou.pRev = pre-import revID
		// _mou.pRev is technically not a part of HLV but it should be updated only when HLV is updated because it is similar to cvCAS
		revID := fmt.Sprintf("\"%v\"", revIdNow)
		ops = append(ops, gocbcore.SubDocOp{
			Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
			Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
			Path:  XATTR_PREVREV,
			Value: []byte(revID),
		})
	}
	// _sync.simCas = macro expandaded (???)
	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
		Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath | memd.SubdocFlagExpandMacros,
		Path:  XATTR_SYNC,
		Value: []byte(xdcrBase.CAS_MACRO_EXPANSION),
	})

	if updateHLV {
		// _vv.cvCas = casNow = pre-import Cas
		ops = append(ops, gocbcore.SubDocOp{
			Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
			Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
			Path:  xdcrCrMeta.XATTR_CVCAS_PATH,
			Value: casNowBytes,
		})
		// _vv.src = bucketUUID
		base64Src, err := xdcrBase.HexToBase64(string(src))
		if err != nil {
			return 0, err
		}
		srcBytes := []byte("\"" + string(base64Src) + "\"")
		ops = append(ops, gocbcore.SubDocOp{
			Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
			Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
			Path:  xdcrCrMeta.XATTR_SRC_PATH,
			Value: []byte(srcBytes),
		})

		// _vv.ver = casNow = pre-import Cas
		ops = append(ops, gocbcore.SubDocOp{
			Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
			Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
			Path:  xdcrCrMeta.XATTR_VER_PATH,
			Value: casNowBytes,
		})

		// _vv.pv = updated pv if cv/mv is rolled over
		if len(pvBytes) > 2 {
			// set new pv
			ops = append(ops, gocbcore.SubDocOp{
				Op:    memd.SubDocOpType(memd.CmdSubDocDictSet),
				Flags: memd.SubdocFlagMkDirP | memd.SubdocFlagXattrPath,
				Path:  xdcrCrMeta.XATTR_PV_PATH,
				Value: pvBytes,
			})
		} else if oldPvLen > 2 {
			// delete old pv
			ops = append(ops, gocbcore.SubDocOp{
				Op:    memd.SubDocOpType(memd.CmdSubDocDelete),
				Flags: memd.SubdocFlagXattrPath,
				Path:  xdcrCrMeta.XATTR_PV_PATH,
				Value: nil,
			})
		}

		// _vv.mv - mv won't exists anymore since we rolled it over to pv
		// remove mv, it is rolled to mv if non-empty
		if oldMvLen > 2 {
			// delete old mv
			ops = append(ops, gocbcore.SubDocOp{
				Op:    memd.SubDocOpType(memd.CmdSubDocDelete),
				Flags: memd.SubdocFlagXattrPath,
				Path:  xdcrCrMeta.XATTR_MV_PATH,
				Value: nil,
			})
		}
	}

	agent.MutateIn(
		gocbcore.MutateInOptions{
			Key:          key,
			Cas:          gocbcore.Cas(casNow),
			Ops:          ops,
			CollectionID: colID,
			Flags:        memd.SubdocDocFlagAccessDeleted,
		}, func(mir *gocbcore.MutateInResult, err error) {
			if err != nil {
				signal <- SubdocSetResult{Err: err}
				return
			}
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
	Pv, Mv                      xdcrHLV.VersionsMap
	OldPvLen, OldMvLen          uint64
	Src                         xdcrHLV.DocumentSourceId
	Ver                         uint64
	CvCas                       uint64
}

func xattrVVtoDeltas(vvBytes []byte) (hlv.VersionsMap, error) {
	vv := make(hlv.VersionsMap)

	if len(vvBytes) == 0 {
		return vv, nil
	}

	it, err := base.NewCCRXattrFieldIterator(vvBytes)
	if err != nil {
		return nil, err
	}
	var lastEntryVersion uint64
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return nil, err
		}
		src := hlv.DocumentSourceId(k)
		ver, err := base.HexLittleEndianToUint64(v)
		if err != nil {
			return nil, err
		}

		lastEntryVersion = ver + lastEntryVersion
		vv[src] = lastEntryVersion
	}

	return vv, nil
}

func GetDocAsOfNow(agent *gocbcore.Agent, key []byte, colID uint32) (cas, sync, revID, importCas uint64, pv, mv xdcrHLV.VersionsMap, oldPvLen, oldMvLen uint64, src xdcrHLV.DocumentSourceId, ver uint64, cvCas uint64, err error) {
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
		Path:  string(XATTR_IMPORTCAS),
	})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrCrMeta.XATTR_PV_PATH)})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrCrMeta.XATTR_MV_PATH)})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrCrMeta.XATTR_SRC_PATH)})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrCrMeta.XATTR_VER_PATH)})

	ops = append(ops, gocbcore.SubDocOp{
		Op:    memd.SubDocOpType(memd.SubDocOpGet),
		Flags: memd.SubdocFlagXattrPath,
		Path:  string(xdcrCrMeta.XATTR_CVCAS_PATH)})

	agent.LookupIn(
		gocbcore.LookupInOptions{
			Key:          key,
			Flags:        memd.SubdocDocFlagAccessDeleted,
			CollectionID: colID,
			Ops:          ops,
		},
		func(lir *gocbcore.LookupInResult, err error) {
			if err != nil {
				signal <- SubdocGetResult{Err: err}
				return
			}

			var err1 error
			var res SubdocGetResult

			// cas
			res.Cas = uint64(lir.Cas)

			// revID
			if lir.Ops[0].Err == nil && len(lir.Ops[0].Value) > 2 {
				revIDStr := string(lir.Ops[0].Value)
				if revid, err1 := strconv.ParseUint(revIDStr[1:len(revIDStr)-1], 10, 64); err1 == nil {
					res.RevId = uint64(revid)
				} else {
					signal <- SubdocGetResult{Err: err1}
					return
				}
			}

			// _sync
			if lir.Ops[1].Err == nil && len(lir.Ops[1].Value) > 2 {
				syncBytes := lir.Ops[1].Value
				sync, err1 = xdcrBase.HexLittleEndianToUint64(syncBytes[1 : len(syncBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.Sync = sync
			}

			// _importCas
			if lir.Ops[2].Err == nil && len(lir.Ops[2].Value) > 2 {
				importCasBytes := lir.Ops[2].Value
				importCas, err1 = xdcrBase.HexLittleEndianToUint64(importCasBytes[1 : len(importCasBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.ImportCas = importCas
			}

			// _vv.pv
			if lir.Ops[3].Err == nil && len(lir.Ops[3].Value) > 2 {
				pvBytes := lir.Ops[3].Value
				pv, err1 = xattrVVtoDeltas(pvBytes)
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.Pv = pv
				res.OldPvLen = uint64(len(pvBytes))
			}

			// _vv.mv
			if lir.Ops[4].Err == nil && len(lir.Ops[4].Value) > 2 {
				mvBytes := lir.Ops[4].Value
				mv, err1 = xattrVVtoDeltas(mvBytes)
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.Mv = mv
				res.OldMvLen = uint64(len(mvBytes))
			}

			// _vv.src
			if lir.Ops[5].Err == nil && len(lir.Ops[5].Value) > 2 {
				srcBytes := lir.Ops[5].Value
				src = xdcrHLV.DocumentSourceId(srcBytes[1 : len(srcBytes)-1])
				res.Src = src
			}

			// _vv.ver
			if lir.Ops[6].Err == nil && len(lir.Ops[6].Value) > 2 {
				verBytes := lir.Ops[6].Value
				ver, err1 = xdcrBase.HexLittleEndianToUint64(verBytes[1 : len(verBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.Ver = ver
			}

			// _vv.cvCas
			if lir.Ops[7].Err == nil && len(lir.Ops[7].Value) > 2 {
				cvCasBytes := lir.Ops[7].Value
				cvCas, err1 = xdcrBase.HexLittleEndianToUint64(cvCasBytes[1 : len(cvCasBytes)-1])
				if err1 != nil {
					signal <- SubdocGetResult{Err: err1}
					return
				}
				res.CvCas = cvCas
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
	oldPvLen = result.OldPvLen
	oldMvLen = result.OldMvLen
	cvCas = result.CvCas
	pv = result.Pv
	mv = result.Mv
	return
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
