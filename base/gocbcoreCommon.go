package base

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	xdcrBase "github.com/couchbase/goxdcr/v8/base"
	xdcrLog "github.com/couchbase/goxdcr/v8/log"

	"github.com/couchbase/gocbcore/v10"
)

type GocbcoreAgentCommon struct {
	Name       string
	Servers    []string
	BucketName string

	SetupTimeout time.Duration
}

type PasswordAuth struct {
	Username string
	Password string
}

type CertificateAuth struct {
	PasswordAuth
	CertificateBytes []byte
}

func (c *CertificateAuth) SupportsTLS() bool {
	return true
}

func (c *CertificateAuth) SupportsNonTLS() bool {
	return false
}

func (c *CertificateAuth) Certificate(req gocbcore.AuthCertRequest) (*tls.Certificate, error) {
	return &tls.Certificate{Certificate: [][]byte{c.CertificateBytes}}, nil
}

func (c *CertificateAuth) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{
		Username: c.Username,
		Password: c.Password,
	}}, nil
}

type RetryStrategy struct{}

func (rs *RetryStrategy) RetryAfter(req gocbcore.RetryRequest,
	reason gocbcore.RetryReason) gocbcore.RetryAction {
	if reason == gocbcore.BucketNotReadyReason {
		return &gocbcore.WithDurationRetryAction{
			WithDuration: gocbcore.ControlledBackoff(req.RetryAttempts()),
		}
	}

	return &gocbcore.NoRetryRetryAction{}
}

func GetConnStr(servers []string) string {
	// for now, http bootstrap only
	connStr := servers[0]
	if connURL, err := url.Parse(servers[0]); err == nil {
		if strings.HasPrefix(connURL.Scheme, "http") {
			// tack on an option: bootstrap_on=http for gocbcore SDK
			// connections to force HTTP config polling
			if ret, err := connURL.Parse("?bootstrap_on=http"); err == nil {
				connStr = ret.String()
			}
		}
	}
	return connStr
}

func TagHttpPrefix(url *string) {
	if !strings.HasPrefix(*url, HttpPrefix) {
		*url = fmt.Sprintf("%v%v", HttpPrefix, *url)
	}
}

var ScramShaAuth = []gocbcore.AuthMechanism{gocbcore.ScramSha1AuthMechanism, gocbcore.ScramSha256AuthMechanism, gocbcore.ScramSha512AuthMechanism}

func GetBucketConnStr(kvVbMap map[string][]uint16, url string, logger *xdcrLog.CommonLogger) string {
	kvHostAddr := ""
	for connStr := range kvVbMap {
		kvHostAddr = connStr
		break
	}
	hostname := xdcrBase.GetHostName(url)
	var kvPort uint16
	var err error
	kvPort, err = xdcrBase.GetPortNumber(kvHostAddr)
	if err != nil {
		logger.Warnf("Error getting kv port. Will use 11210. kvVbMap=%v, url=%v", kvVbMap, url)
		kvPort = 11210
	}

	return fmt.Sprintf("%v%v:%v", CouchbasePrefix, hostname, kvPort)
}
