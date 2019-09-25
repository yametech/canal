package canal

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"time"
)

// DialOption specifies an option for dialing a Redis server.
type DialOption struct {
	f func(*dialOptions)
}

type dialOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialer       *net.Dialer
	dial         func(network, addr string) (net.Conn, error)
	password     string
	useTLS       bool
	skipVerify   bool
	tlsConfig    *tls.Config
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

// DialConnectTimeout specifies the timeout for connecting to the Redis server when
// no DialNetDial option is specified.
func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.Timeout = d
	}}
}

// DialKeepAlive specifies the keep-alive period for TCP connections to the Redis server
// when no DialNetDial option is specified.
// If zero, keep-alives are not enabled. If no DialKeepAlive option is specified then
// the default of 5 minutes is used to ensure that half-closed TCP sessions are detected.
func DialKeepAlive(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialNetDial overrides DialConnectTimeout and DialKeepAlive.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dial = dial
	}}
}

// DialPassword specifies the password to use when connecting to
// the Redis server.
func DialPassword(password string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.password = password
	}}
}

// DialTLSConfig specifies the config to use when a TLS connection is dialed.
// Has no effect when not dialing a TLS connection.
func DialTLSConfig(c *tls.Config) DialOption {
	return DialOption{func(do *dialOptions) {
		do.tlsConfig = c
	}}
}

// DialTLSSkipVerify disables server name verification when connecting over
// TLS. Has no effect when not dialing a TLS connection.
func DialTLSSkipVerify(skip bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.skipVerify = skip
	}}
}

// DialUseTLS specifies whether TLS should be used when connecting to the
// server. This option is ignore by DialURL.
func DialUseTLS(useTLS bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.useTLS = useTLS
	}}
}

// dial connects to the Redis server at the given network and
// address using the specified options.
func dial(network, address string, options ...DialOption) (
	netConn net.Conn,
	err error,
) {
	do := dialOptions{
		dialer: &net.Dialer{
			KeepAlive: time.Hour * 864000,
		},
	}
	for _, option := range options {
		option.f(&do)
	}

	if do.dial == nil {
		do.dial = do.dialer.Dial
	}

	if netConn, err = do.dial(network, address); err != nil {
		return nil, err
	}

	if do.useTLS {
		var tlsConfig *tls.Config
		if do.tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: do.skipVerify,
			}
		} else {
			tlsConfig = cloneTLSConfig(do.tlsConfig)
		}
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				netConn.Close()
				return nil, err
			}
			tlsConfig.ServerName = host
		}

		tlsConn := tls.Client(netConn, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			netConn.Close()
			return nil, err
		}
		netConn = tlsConn

	}

	rd := newReader(netConn)
	wr := newWriter(netConn)

	if do.password != "" {
		if err := wr.writeMultiBulk("AUTH", do.password); err != nil {
			return nil, err
		}
		val, _, err := rd.readBulk()
		if err != nil {
			netConn.Close()
			return nil, err
		}
		if !bytes.Equal(val.Str, []byte("OK")) {
			netConn.Close()
			return nil, errors.New("auth error")
		}
	}

	// Verify successful first ping
	if err := wr.writeMultiBulk("PING"); err != nil {
		return nil, err
	}
	val, _, err := rd.readBulk()
	if err != nil {
		netConn.Close()
		return nil, err
	}
	if !bytes.Equal(val.Str, []byte("PONG")) {
		netConn.Close()
		return nil, errors.New(val.String())
	}

	return netConn, nil
}
