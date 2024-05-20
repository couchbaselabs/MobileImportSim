// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package utils

import (
	"fmt"
	"math"
	mrand "math/rand"
	"mobileImportSim/base"
	"sort"
	"strings"
	"sync"
	"time"

	xdcrLog "github.com/couchbase/goxdcr/log"
)

// evenly distribute load across workers
// assumes that num_of_worker <= num_of_load
// returns load_distribution [][]int, where
//
//	load_distribution[i][0] is the start index, inclusive, of load for ith worker
//	load_distribution[i][1] is the end index, exclusive, of load for ith worker
//
// note that load is zero indexed, i.e., indexed as 0, 1, .. N-1 for N loads
func BalanceLoad(num_of_worker int, num_of_load int) [][]int {
	load_distribution := make([][]int, 0)

	max_load_per_worker := int(math.Ceil(float64(num_of_load) / float64(num_of_worker)))
	num_of_worker_with_max_load := num_of_load - (max_load_per_worker-1)*num_of_worker

	index := 0
	var num_of_load_per_worker int
	for i := 0; i < num_of_worker; i++ {
		if i < num_of_worker_with_max_load {
			num_of_load_per_worker = max_load_per_worker
		} else {
			num_of_load_per_worker = max_load_per_worker - 1
		}

		load_for_worker := make([]int, 2)
		load_for_worker[0] = index
		index += num_of_load_per_worker
		load_for_worker[1] = index

		load_distribution = append(load_distribution, load_for_worker)
	}

	if index != num_of_load {
		panic(fmt.Sprintf("number of load processed %v does not match total number of load %v", index, num_of_load))
	}

	return load_distribution
}

func WaitForWaitGroup(waitGroup *sync.WaitGroup, doneChan chan bool) {
	waitGroup.Wait()
	close(doneChan)
}

type ExponentialOpFunc func() error

/**
 * Executes a anonymous function that returns an error. If the error is non nil, retry with exponential backoff.
 * Returns base.ErrorFailedAfterRetry + the last recorded error if operation times out, nil otherwise.
 * Max retries == the times to retry in additional to the initial try, should the initial try fail
 * initialWait == Initial time with which to start
 * Factor == exponential backoff factor based off of initialWait
 */
func ExponentialBackoffExecutor(name string, initialWait time.Duration, maxRetries int, factor int, maxBackoff time.Duration, op ExponentialOpFunc, logger *xdcrLog.CommonLogger) error {
	waitTime := initialWait
	var opErr error
	for i := 0; i <= maxRetries; i++ {
		opErr = op()
		if opErr == nil {
			return nil
		} else if i != maxRetries {
			logger.Infof("SimulateImport for %v failed with %v. retry=%v. Will retry after %v\n", name, opErr, i, waitTime)
			time.Sleep(waitTime)
			waitTime *= time.Duration(factor)
			if waitTime > maxBackoff {
				waitTime = maxBackoff
			}
		}
	}
	// opErr = fmt.Errorf("%v Operation failed after max retries. Last error: %v", name, opErr.Error())
	logger.Infof("Retrying the op until it is successful with retry interval %v", waitTime)
	count := maxRetries + 1
	for {
		opErr = op()
		if opErr == nil {
			logger.Infof("The operation succeeded after %v retries", count)
			break
		}
		time.Sleep(waitTime)
		count++
	}
	return opErr
}

// add to error chan without blocking
func AddToErrorChan(errChan chan error, err error) {
	select {
	case errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

func DeepCopyUint16Array(in []uint16) []uint16 {
	if in == nil {
		return nil
	}

	out := make([]uint16, len(in))
	copy(out, in)
	return out
}

func ShuffleVbList(list []uint16) {
	r := mrand.New(mrand.NewSource(time.Now().Unix()))
	// Start at the end of the slice, go backwards and scramble
	for i := len(list); i > 1; i-- {
		randIndex := r.Intn(i)
		// Swap values and continue until we're done
		if (i - 1) != randIndex {
			list[i-1], list[randIndex] = list[randIndex], list[i-1]
		}
	}
}

func PopulateCCCPConnectString(url string) string {
	var cccpUrl string

	if !strings.HasPrefix(cccpUrl, base.CouchbasePrefix) {
		cccpUrl = fmt.Sprintf("%v%v", base.CouchbasePrefix, cccpUrl)
	}
	return cccpUrl
}

// type to facilitate the sorting of uint16 lists
type Uint8List []uint8

func (u Uint8List) Len() int           { return len(u) }
func (u Uint8List) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u Uint8List) Less(i, j int) bool { return u[i] < u[j] }

func SortUint8List(list []uint8) []uint8 {
	sort.Sort(Uint8List(list))
	return list
}

func SearchUint8List(seqno_list []uint8, seqno uint8) (int, bool) {
	index := sort.Search(len(seqno_list), func(i int) bool {
		return seqno_list[i] >= seqno
	})
	if index < len(seqno_list) && seqno_list[index] == seqno {
		return index, true
	} else {
		return index, false
	}
}
