package dcp

import (
	"errors"
	"fmt"
	"math"
	"mobileImportSim/base"
	"mobileImportSim/utils"
	"sync"
	"sync/atomic"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	gocbcore "github.com/couchbase/gocbcore/v10"
	xdcrLog "github.com/couchbase/goxdcr/v8/log"
)

type DcpClient struct {
	Name               string
	dcpDriver          *DcpDriver
	vbList             []uint16
	cluster            *gocb.Cluster
	dcpAgent           *gocbcore.DCPAgent
	waitGroup          *sync.WaitGroup
	dcpHandlers        []*DcpHandler
	vbHandlerMap       map[uint16]*DcpHandler
	numberClosing      uint32
	closeStreamsDoneCh chan bool
	activeStreams      uint32
	finChan            chan bool
	bufferCap          int
	gocbcoreDcpFeed    *GocbcoreDCPFeed
	url                string
	username, password string
}

func NewDcpClient(dcpDriver *DcpDriver, i int, vbList []uint16, waitGroup *sync.WaitGroup, bufferCap int, username, password, url string) *DcpClient {
	return &DcpClient{
		Name:               fmt.Sprintf("%v_%v", dcpDriver.Name, i),
		dcpDriver:          dcpDriver,
		vbList:             vbList,
		waitGroup:          waitGroup,
		dcpHandlers:        make([]*DcpHandler, dcpDriver.numberOfWorkers),
		vbHandlerMap:       make(map[uint16]*DcpHandler),
		closeStreamsDoneCh: make(chan bool),
		finChan:            make(chan bool),
		bufferCap:          bufferCap,
		username:           username,
		password:           password,
		url:                url,
	}
}

func (c *DcpClient) Start() error {
	c.dcpDriver.logger.Infof("Dcp client %v starting", c.Name)
	defer c.dcpDriver.logger.Infof("Dcp client %v started", c.Name)

	err := c.initialize()
	if err != nil {
		return err
	}

	go c.handleDcpStreams()

	return nil
}

func (c *DcpClient) reportActiveStreams() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			activeStreams := atomic.LoadUint32(&c.activeStreams)
			c.dcpDriver.logger.Infof("%v active streams=%v", c.Name, activeStreams)
			if activeStreams == uint32(len(c.vbList)) {
				c.dcpDriver.logger.Infof("%v all streams active. Stop reporting", c.Name)
				goto done
			}
		case <-c.finChan:
			goto done
		}
	}
done:
}

func (c *DcpClient) closeStreamIfOpen(vbno uint16) {
	vbState := c.dcpDriver.getVbState(vbno)
	if vbState != VBStateStreamClosed {
		err := c.closeStream(vbno)
		if err == nil {
			c.dcpDriver.setVbState(vbno, VBStateStreamClosed)
		}
	}

}

func (c *DcpClient) Stop() error {
	c.dcpDriver.logger.Infof("Dcp client %v stopping", c.Name)
	defer c.dcpDriver.logger.Infof("Dcp client %v stopped", c.Name)

	defer c.waitGroup.Done()

	close(c.finChan)

	c.numberClosing = uint32(len(c.vbList))
	for _, i := range c.vbList {
		c.closeStreamIfOpen(i)
	}

	c.dcpDriver.logger.Infof("Dcp client %v stopping handlers", c.Name)
	for _, dcpHandler := range c.dcpHandlers {
		if dcpHandler != nil {
			dcpHandler.Stop()
		}
	}
	c.dcpDriver.logger.Infof("Dcp client %v done stopping handlers", c.Name)

	return nil
}

func (c *DcpClient) initialize() error {
	err := c.initializeCluster()
	if err != nil {
		c.dcpDriver.logger.Errorf("Error initializing cluster %v - %v", c.Name, err)
		return err
	}

	err = c.initializeBucket()
	if err != nil {
		c.dcpDriver.logger.Errorf("Error initializing bucket %v - %v", c.Name, err)
		return err
	}

	err = c.initializeDcpHandlers()
	if err != nil {
		c.dcpDriver.logger.Errorf("Error initializing DCP Handlers %v - %v", c.Name, err)
		return err
	}

	return nil
}

func (c *DcpClient) initializeCluster() (err error) {
	cluster, err := initializeClusterWithSecurity(c.dcpDriver, c.dcpDriver.logger)
	if err != nil {
		return err
	}

	c.cluster = cluster
	return nil
}

func initializeClusterWithSecurity(dcpDriver *DcpDriver, logger *xdcrLog.CommonLogger) (*gocb.Cluster, error) {
	clusterOpts := gocb.ClusterOptions{}
	clusterOpts.Authenticator = gocb.PasswordAuthenticator{
		Username: dcpDriver.username,
		Password: dcpDriver.password,
	}

	cluster, err := gocb.Connect(utils.PopulateCCCPConnectString(dcpDriver.url), clusterOpts)
	if err != nil {
		logger.Errorf("Error connecting to cluster %v. err=%v", dcpDriver.url, err)
		return nil, err
	}
	return cluster, nil
}

func (c *DcpClient) initializeBucket() (err error) {
	auth := &base.PasswordAuth{
		Username: c.dcpDriver.username,
		Password: c.dcpDriver.password,
	}

	c.gocbcoreDcpFeed, err = NewGocbcoreDCPFeed(c.Name, []string{c.dcpDriver.bucketConnStr}, c.dcpDriver.bucketName, auth, false)
	return
}

func (c *DcpClient) initializeDcpHandlers() error {
	loadDistribution := utils.BalanceLoad(c.dcpDriver.numberOfWorkers, len(c.vbList))
	for i := 0; i < c.dcpDriver.numberOfWorkers; i++ {
		lowIndex := loadDistribution[i][0]
		highIndex := loadDistribution[i][1]
		vbList := make([]uint16, highIndex-lowIndex)
		for j := lowIndex; j < highIndex; j++ {
			vbList[j-lowIndex] = c.vbList[j]
		}

		dcpHandler, err := NewDcpHandler(c, i, vbList, c.dcpDriver.numberOfBins,
			c.dcpDriver.dcpHandlerChanSize, c.dcpDriver.IncrementDocReceived,
			c.dcpDriver.IncrementSysOrUnsubbedEventReceived, c.bufferCap)
		if err != nil {
			c.dcpDriver.logger.Errorf("Error constructing dcp handler. err=%v", err)
			return err
		}

		err = dcpHandler.Start()
		if err != nil {
			c.dcpDriver.logger.Errorf("Error starting dcp handler. err=%v", err)
			return err
		}

		c.dcpHandlers[i] = dcpHandler

		for j := lowIndex; j < highIndex; j++ {
			c.vbHandlerMap[c.vbList[j]] = dcpHandler
		}
	}
	return nil
}

func (c *DcpClient) handleDcpStreams() {
	err := c.openDcpStreams()
	if err != nil {
		wrappedErr := fmt.Errorf("%v: %v", c.Name, err.Error())
		c.reportError(wrappedErr)
		return
	}

	go c.reportActiveStreams()
}

func (c *DcpClient) openDcpStreams() error {
	//randomize to evenly distribute [initial] load to handlers
	vbListCopy := utils.DeepCopyUint16Array(c.vbList)
	utils.ShuffleVbList(vbListCopy)
	for _, vbno := range vbListCopy {
		if c.dcpAgent == nil {
			c.dcpAgent = c.gocbcoreDcpFeed.dcpAgent
		}

		var vbUUID gocbcore.VbUUID
		errCh := make(chan error)
		_, err := c.dcpAgent.GetFailoverLog(vbno, func(fe []gocbcore.FailoverEntry, errIn error) {
			if errIn != nil {
				errCh <- errIn
				return
			}
			if len(fe) < 1 {
				c.dcpDriver.logger.Errorf("Error getting failover log for vb %v", vbno)
				errCh <- errors.New("no failover logs for vb")
				return
			}
			vbUUID = fe[0].VbUUID
			errCh <- errIn
		})
		if err != nil {
			c.dcpDriver.logger.Errorf("err calling get failover log for vb %v. err=%v", vbno, err)
			return err
		}
		err = <-errCh
		if err != nil {
			c.dcpDriver.logger.Errorf("err getting failover log for vb %v. err=%v", vbno, err)
			return err
		}
		_, err = c.dcpAgent.OpenStream(vbno, 0, vbUUID, gocbcore.SeqNo(0),
			gocbcore.SeqNo(math.MaxUint64 /*vbts.EndSeqno*/), gocbcore.SeqNo(0), gocbcore.SeqNo(0), c.vbHandlerMap[vbno],
			gocbcore.OpenStreamOptions{}, c.openStreamFunc)

		if err != nil {
			c.dcpDriver.logger.Errorf("err opening dcp stream for vb %v. err=%v", vbno, err)
			return err
		}
	}

	return nil
}

func (c *DcpClient) closeStream(vbno uint16) error {
	var err error
	if c.dcpAgent != nil {
		_, err = c.dcpAgent.CloseStream(vbno, gocbcore.CloseStreamOptions{}, c.closeStreamFunc)
		if err != nil {
			c.dcpDriver.logger.Errorf("%v error stopping dcp stream for vb %v. err=%v", c.Name, vbno, err)
		}
	}
	return err
}

func (c *DcpClient) openStreamFunc(f []gocbcore.FailoverEntry, err error) {
	if err != nil {
		wrappedErr := fmt.Errorf("%v openStreamCallback reported err: %v", c.Name, err)
		c.reportError(wrappedErr)
	} else {
		atomic.AddUint32(&c.activeStreams, 1)
	}
}

func (c *DcpClient) reportError(err error) {
	select {
	case c.dcpDriver.errChan <- err:
	default:
		// some error already sent to errChan. no op
	}
}

// CloseStreamCallback
func (c *DcpClient) closeStreamFunc(err error) {
	// (-1)
	streamsLeft := atomic.AddUint32(&c.numberClosing, ^uint32(0))
	if streamsLeft == 0 {
		c.closeStreamsDoneCh <- true
	}
}
