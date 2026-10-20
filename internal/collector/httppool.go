package collector

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/jan-benedikt/rabbitmq-unified-exporter/internal/config"
)

// httpPool caches *http.Client by TLS/timeout identity so scrapes reuse connections.
// Cleared on config reload (CA/cert path changes).
type httpPool struct {
	mu      sync.Mutex
	clients map[string]*http.Client
}

func newHTTPPool() *httpPool {
	return &httpPool{clients: map[string]*http.Client{}}
}

func (p *httpPool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		if tr, ok := c.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	p.clients = map[string]*http.Client{}
}

func (p *httpPool) Client(key string, timeout time.Duration, tlsCfg *tls.Config) *http.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[key]; ok {
		return c
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}
	c := &http.Client{
		Timeout:   timeout,
		Transport: tr,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	p.clients[key] = c
	return c
}

func tlsPoolKey(kind, targetID string, timeout time.Duration, tlsCfg config.TLSConfig) string {
	return fmt.Sprintf("%s|%s|%s|%t|%t|%s|%s|%s|%s",
		kind, targetID, timeout,
		tlsCfg.Enabled, tlsCfg.InsecureSkipVerify,
		tlsCfg.CAFile, tlsCfg.CertFile, tlsCfg.KeyFile, tlsCfg.ServerName,
	)
}
