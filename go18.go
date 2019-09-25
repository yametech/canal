// +build go1.8

package canal

import "crypto/tls"

func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	return cfg.Clone()
}
