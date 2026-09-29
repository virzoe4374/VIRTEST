package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// BERMUDA Stealth Gateway NG — L7 Streaming Reverse Proxy & Camouflage Shield
// High-Throughput Quantitative Pipeline & Macro Egress Adapter (Phase 1 Target)
// 64KiB Zero-Allocation Channel Free-List, Hijack Tracking & OpenResty Emulation
// ---------------------------------------------------------------------------

const (
	defaultPathXH = "/bermuda-xhttp"
	defaultPathWS = "/bermuda-ws"
	defaultPathTR = "/bermuda-tr"

	// openrestyServerToken represents the hardened 'server_tokens off' posture.
	openrestyServerToken = "openresty"

	// Authentic OpenResty 1.19.9.1 RPM index.html metadata:
	// size = 1097 bytes = 0x449, mtime = 1628285554 = 0x610daa72
	openrestyIndexLastModified = "Fri, 06 Aug 2021 21:32:34 GMT"
	openrestyIndexETag         = "\"610daa72-449\""

	// Authentic OpenResty 1.19.9.1 RPM 50x.html metadata:
	// size = 982 bytes = 0x3d6 (served for 502 Bad Gateway via error_page)
	openresty50xLastModified = openrestyIndexLastModified
	openresty50xETag         = "\"610daa72-3d6\""

	// 64KiB pooled buffer slabs maximize line-rate throughput for high-concurrency
	// bursts and halve chunked framing overhead on multi-threaded parallel downloads.
	defaultProxyBufferSize  = 64 * 1024
	defaultTransportBufSize = 64 * 1024
)

// ---------------------------------------------------------------------------
// Authentic OpenResty HTML bodies (byte-exact official distribution artifacts)
// ---------------------------------------------------------------------------

// openrestyWelcomeHTML: official openresty-1.19.9.1 RPM html/index.html (1097 bytes, LF)
const openrestyWelcomeHTML = "<!DOCTYPE html>\n" +
	"<html>\n" +
	"<head>\n" +
	"<meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\">\n" +
	"<meta content=\"utf-8\" http-equiv=\"encoding\">\n" +
	"<title>Welcome to OpenResty!</title>\n" +
	"<style>\n" +
	"    body {\n" +
	"        width: 35em;\n" +
	"        margin: 0 auto;\n" +
	"        font-family: Tahoma, Verdana, Arial, sans-serif;\n" +
	"    }\n" +
	"</style>\n" +
	"</head>\n" +
	"<body>\n" +
	"<h1>Welcome to OpenResty!</h1>\n" +
	"<p>If you see this page, the OpenResty web platform is successfully installed and\n" +
	"working. Further configuration is required.</p>\n" +
	"\n" +
	"<p>For online documentation and support please refer to our\n" +
	"<a href=\"https://openresty.org/\">openresty.org</a> site<br/>\n" +
	"Commercial support is available at\n" +
	"<a href=\"https://openresty.com/\">openresty.com</a>.</p>\n" +
	"<p>We have articles on troubleshooting issues like <a href=\"https://blog.openresty.com/en/lua-cpu-flame-graph/?src=wb\">high CPU usage</a> and\n" +
	"<a href=\"https://blog.openresty.com/en/how-or-alloc-mem/\">large memory usage</a> on <a href=\"https://blog.openresty.com/\">our official blog site</a>.\n" +
	"<p><em>Thank you for flying <a href=\"https://openresty.org/\">OpenResty</a>.</em></p>\n" +
	"</body>\n" +
	"</html>\n"

// openresty50xHTML: official openresty-1.19.9.1 RPM html/50x.html (982 bytes, LF)
const openresty50xHTML = "<!DOCTYPE html>\n" +
	"<html>\n" +
	"<head>\n" +
	"<meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\">\n" +
	"<meta content=\"utf-8\" http-equiv=\"encoding\">\n" +
	"<title>Error</title>\n" +
	"<style>\n" +
	"    body {\n" +
	"        width: 35em;\n" +
	"        margin: 0 auto;\n" +
	"        font-family: Tahoma, Verdana, Arial, sans-serif;\n" +
	"    }\n" +
	"</style>\n" +
	"</head>\n" +
	"<body>\n" +
	"<h1>An error occurred.</h1>\n" +
	"<p>Sorry, the page you are looking for is currently unavailable.<br/>\n" +
	"Please try again later.</p>\n" +
	"<p>If you are the system administrator of this resource then you should check\n" +
	"the <a href=\"http://nginx.org/r/error_log\">error log</a> for details.</p>\n" +
	"<p>We have articles on troubleshooting issues like <a href=\"https://blog.openresty.com/en/lua-cpu-flame-graph/?src=wb\">high CPU usage</a> and\n" +
	"<a href=\"https://blog.openresty.com/en/how-or-alloc-mem/\">large memory usage</a> on <a href=\"https://blog.openresty.com/\">our official blog site</a>.\n" +
	"<p><em>Faithfully yours, <a href=\"https://openresty.org/\">OpenResty</a>.</em></p>\n" +
	"</body>\n" +
	"</html>\n"

// openresty404HTML: nginx built-in 404 template (ngx_http_special_response.c, CRLF)
const openresty404HTML = "<html>\r\n" +
	"<head><title>404 Not Found</title></head>\r\n" +
	"<body>\r\n" +
	"<center><h1>404 Not Found</h1></center>\r\n" +
	"<hr><center>openresty</center>\r\n" +
	"</body>\r\n" +
	"</html>\r\n"

// openresty403HTML: nginx built-in 403 template (ngx_http_special_response.c, CRLF)
const openresty403HTML = "<html>\r\n" +
	"<head><title>403 Forbidden</title></head>\r\n" +
	"<body>\r\n" +
	"<center><h1>403 Forbidden</h1></center>\r\n" +
	"<hr><center>openresty</center>\r\n" +
	"</body>\r\n" +
	"</html>\r\n"

// openresty405HTML: nginx built-in 405 template (ngx_http_special_response.c, CRLF)
const openresty405HTML = "<html>\r\n" +
	"<head><title>405 Not Allowed</title></head>\r\n" +
	"<body>\r\n" +
	"<center><h1>405 Not Allowed</h1></center>\r\n" +
	"<hr><center>openresty</center>\r\n" +
	"</body>\r\n" +
	"</html>\r\n"

// ---------------------------------------------------------------------------
// Zero-Allocation Bounded Channel Buffer Pool (httputil.BufferPool interface)
// ---------------------------------------------------------------------------

// bufferPoolCapacity bounds resident pool memory to exactly 256 x 64KiB = 16MiB.
const bufferPoolCapacity = 256

type recycledBufferPool struct {
	size int
	ch   chan []byte
}

func newRecycledBufferPool(size int) *recycledBufferPool {
	return &recycledBufferPool{
		size: size,
		ch:   make(chan []byte, bufferPoolCapacity),
	}
}

func (p *recycledBufferPool) Get() []byte {
	select {
	case b := <-p.ch:
		return b
	default:
		return make([]byte, p.size)
	}
}

func (p *recycledBufferPool) Put(b []byte) {
	if cap(b) != p.size {
		return // Ignore foreign or resized slab
	}
	b = b[:cap(b)]
	select {
	case p.ch <- b:
	default: // Free list full; allow GC to reclaim slab
	}
}

// ---------------------------------------------------------------------------
// Telemetry & Health Models
// ---------------------------------------------------------------------------

type HealthResponse struct {
	Status     string                   `json:"status"`
	Draining   bool                     `json:"draining"`
	Healthy    bool                     `json:"healthy"`
	UptimeSec  int64                    `json:"uptime_sec"`
	Supervisor SupervisorHealthSnapshot `json:"supervisor"`
	Telemetry  TelemetrySnapshot        `json:"telemetry"`
}

type TelemetrySnapshot struct {
	OpenConnections int64 `json:"open_connections"`
	ActiveTunnels   int64 `json:"active_tunnels"`
	TunnelsOpened   int64 `json:"tunnels_opened_total"`
	TotalRequests   int64 `json:"total_requests"`
}

// Gateway encapsulates edge reverse-proxy routing, connection tracking,
// health endpoints, hijack-aware tunnel registry, and camouflage shield.
type Gateway struct {
	sup       *Supervisor
	pathXH    string
	pathWS    string
	pathTR    string
	backendXH string
	backendWS string
	backendTR string

	xhProxy *httputil.ReverseProxy
	wsProxy *httputil.ReverseProxy
	trProxy *httputil.ReverseProxy

	tr       *http.Transport
	bufPool  *recycledBufferPool
	draining atomic.Bool

	openConns   atomic.Int64
	totalReq    atomic.Int64
	tunnelsLive atomic.Int64
	tunnelsHit  atomic.Int64

	tunnelsMu sync.Mutex
	tunnels   map[net.Conn]struct{}

	startedAt time.Time
}

// NewGateway constructs the production gateway with 64KiB pooled slabs.
func NewGateway(sup *Supervisor) *Gateway {
	return newGateway(sup, defaultProxyBufferSize, defaultTransportBufSize)
}

func newGateway(sup *Supervisor, proxyBuf, transportBuf int) *Gateway {
	pathXH := getEnv("BERMUDA_PATH_XH", defaultPathXH)
	pathWS := getEnv("BERMUDA_PATH_WS", defaultPathWS)
	pathTR := getEnv("BERMUDA_PATH_TR", defaultPathTR)

	backendXH := getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH)
	backendWS := getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS)
	backendTR := getEnv("BERMUDA_BACKEND_TR", defaultLoopbackTR)

	sharedPool := newRecycledBufferPool(proxyBuf)
	tr := newLoopbackTransport(transportBuf)

	xhProxy, err := newBackendProxy(backendXH, tr, sharedPool)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: invalid XHTTP backend address %q: %v", backendXH, err)
	}
	wsProxy, err := newBackendProxy(backendWS, tr, sharedPool)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: invalid WS backend address %q: %v", backendWS, err)
	}
	trProxy, err := newBackendProxy(backendTR, tr, sharedPool)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: invalid Trojan backend address %q: %v", backendTR, err)
	}

	return &Gateway{
		sup:       sup,
		pathXH:    pathXH,
		pathWS:    pathWS,
		pathTR:    pathTR,
		backendXH: backendXH,
		backendWS: backendWS,
		backendTR: backendTR,
		xhProxy:   xhProxy,
		wsProxy:   wsProxy,
		trProxy:   trProxy,
		tr:        tr,
		bufPool:   sharedPool,
		tunnels:   make(map[net.Conn]struct{}),
		startedAt: time.Now(),
	}
}

// newLoopbackTransport builds a churn-free transport tuned for high-concurrency burst scraping
// across 101 parallel channels and long-tail macroeconomic data feeds.
func newLoopbackTransport(bufSize int) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second, // Headroom for container-scheduler jitter
		KeepAlive: -1,              // Disable keepalives on local loopback to avoid socket accumulation
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			_ = c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			})
			return ctrlErr
		},
	}

	return &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          1024,              // Upgraded from 768 to 1024: absorbs 101 parallel channel bursts with zero churn
		MaxIdleConnsPerHost:   512,               // Upgraded from 256 to 512: eliminates ephemeral port depletion during heavy 47-feed waves
		MaxConnsPerHost:       512,               // Upgraded from 256 to 512: unlocks unthrottled loopback multiplexing
		IdleConnTimeout:       150 * time.Second, // Upgraded from 75s to 150s: keeps sockets warm across inter-wave pauses in FX Foresight
		DisableCompression:    true,              // Avoid CPU burn and preserve chunked framing
		ReadBufferSize:        bufSize,
		WriteBufferSize:       bufSize,
		ResponseHeaderTimeout: 120 * time.Second, // Upgraded from 60s to 120s: critical fix preventing 502 Bad Gateway on long-tail feeds (P6B, P6 up to 75s)
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// newBackendProxy instantiates a ReverseProxy configured for zero-latency streaming.
func newBackendProxy(targetAddr string, tr http.RoundTripper, bp httputil.BufferPool) (*httputil.ReverseProxy, error) {
	targetURL, err := url.Parse("http://" + targetAddr)
	if err != nil {
		return nil, err
	}
	if targetURL.Host == "" {
		return nil, errors.New("missing target host")
	}

	return &httputil.ReverseProxy{
		Transport:     tr,
		BufferPool:    bp,
		FlushInterval: -1, // Synchronous immediate flush for line-rate streaming
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = targetURL.Scheme
			pr.Out.URL.Host = targetURL.Host
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
			pr.Out.Header.Set("X-Accel-Buffering", "no")
		},
		ModifyResponse: func(res *http.Response) error {
			// In Go 1.24, ModifyResponse also runs for 101 Switching Protocols.
			// Guard against polluting WebSocket handshake headers.
			if res.StatusCode != http.StatusSwitchingProtocols {
				res.Header.Set("X-Accel-Buffering", "no")
				res.Header.Set("Server", openrestyServerToken)
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("[Proxy Error] Backend %s unreachable: %v", targetAddr, err)
			}
			camoOpenResty(w, r, http.StatusBadGateway, openresty50xHTML, map[string]string{
				"Last-Modified": openresty50xLastModified,
				"ETag":          openresty50xETag,
				"Accept-Ranges": "bytes",
			})
		},
	}, nil
}

func (g *Gateway) SetDraining() {
	g.draining.Store(true)
}

func (g *Gateway) IsDraining() bool {
	return g.draining.Load()
}

func (g *Gateway) CloseIdleBackendConns() {
	if g.tr != nil {
		g.tr.CloseIdleConnections()
	}
}

// CloseTunnels force-closes every active hijacked WebSocket tunnel connection.
func (g *Gateway) CloseTunnels() {
	g.tunnelsMu.Lock()
	conns := make([]net.Conn, 0, len(g.tunnels))
	for c := range g.tunnels {
		conns = append(conns, c)
	}
	g.tunnelsMu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (g *Gateway) registerTunnel(c *trackedConn) {
	g.tunnelsMu.Lock()
	g.tunnels[c] = struct{}{}
	g.tunnelsMu.Unlock()
	g.tunnelsLive.Add(1)
	g.tunnelsHit.Add(1)
}

func (g *Gateway) unregisterTunnel(c *trackedConn) {
	g.tunnelsMu.Lock()
	delete(g.tunnels, c)
	g.tunnelsMu.Unlock()
	g.tunnelsLive.Add(-1)
}

func (g *Gateway) TrackConnState(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		g.openConns.Add(1)
	case http.StateClosed:
		g.openConns.Add(-1)
	case http.StateHijacked:
		g.openConns.Add(-1)
	}
}

// Handler returns the primary HTTP multiplexer implementing hot-path proxying and decoys.
func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.totalReq.Add(1)

		// 1. Healthcheck Endpoint (Railway Orchestration Probe)
		if r.URL.Path == "/healthz" {
			g.handleHealthz(w, r)
			return
		}

		// 2. VLESS-XHTTP Hot-Path (Direct native ResponseWriter, zero wrapper overhead)
		if isProxyPath(r.URL.Path, g.pathXH) {
			g.xhProxy.ServeHTTP(w, r)
			return
		}

		// 3. VLESS-WS Hot-Path (Hijack-tracking pass-through for 101 Switching Protocols)
		if isProxyPath(r.URL.Path, g.pathWS) {
			g.wsProxy.ServeHTTP(&hijackTrackingWriter{ResponseWriter: w, gw: g}, r)
			return
		}

		// 4. Trojan-WS Hot-Path (Hijack-tracking pass-through for 101 Switching Protocols)
		if isProxyPath(r.URL.Path, g.pathTR) {
			g.trProxy.ServeHTTP(&hijackTrackingWriter{ResponseWriter: w, gw: g}, r)
			return
		}

		// 5. Camouflage & Active Probe Neutralization Layer
		g.serveCamouflage(w, r)
	})
}

func isProxyPath(p, base string) bool {
	return p == base || strings.HasPrefix(p, base+"/")
}

func (g *Gateway) serveCamouflage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		camoOpenResty(w, r, http.StatusMethodNotAllowed, openresty405HTML, nil)
		return
	}

	cleanPath := path.Clean("/" + r.URL.Path)

	switch {
	case cleanPath == "/" || cleanPath == "/index.html":
		if requestNotModified(r, openrestyIndexETag, openrestyIndexLastModified) {
			camoNotModified(w, r, openrestyIndexETag, openrestyIndexLastModified)
			return
		}
		camoOpenResty(w, r, http.StatusOK, openrestyWelcomeHTML, map[string]string{
			"Last-Modified": openrestyIndexLastModified,
			"ETag":          openrestyIndexETag,
			"Accept-Ranges": "bytes",
		})
	case strings.HasPrefix(cleanPath, "/."):
		// Stock OpenResty behavior: no dotfile deny rule exists; return standard 404
		camoOpenResty(w, r, http.StatusNotFound, openresty404HTML, nil)
	default:
		camoOpenResty(w, r, http.StatusNotFound, openresty404HTML, nil)
	}
}

func requestNotModified(r *http.Request, etag, lastModified string) bool {
	if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" {
		if inm == "*" {
			return true
		}
		for _, candidate := range strings.Split(inm, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), etag) {
				return true
			}
		}
		return false
	}
	ims := strings.TrimSpace(r.Header.Get("If-Modified-Since"))
	if ims == "" {
		return false
	}
	t, err := http.ParseTime(ims)
	if err != nil {
		return false
	}
	lm, err := http.ParseTime(lastModified)
	if err != nil {
		return false
	}
	return t.Equal(lm) // Nginx default 'if_modified_since exact'
}

func (g *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		camoOpenResty(w, r, http.StatusMethodNotAllowed, openresty405HTML, nil)
		return
	}

	snap := g.sup.Snapshot()
	snap.ChildPID = 0 // Omit child PID to avoid exposing internal topology

	draining := g.draining.Load()
	healthy := !draining && snap.Running && snap.Ready

	statusStr := "ok"
	code := http.StatusOK
	switch {
	case draining:
		code = http.StatusServiceUnavailable
		statusStr = "draining"
	case !snap.Running:
		code = http.StatusServiceUnavailable
		statusStr = "down"
	case !snap.Ready:
		code = http.StatusServiceUnavailable
		statusStr = "starting"
	}

	w.Header().Set("Server", openrestyServerToken)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.WriteHeader(code)

	payload := HealthResponse{
		Status:     statusStr,
		Draining:   draining,
		Healthy:    healthy,
		UptimeSec:  int64(time.Since(g.startedAt).Seconds()),
		Supervisor: snap,
		Telemetry: TelemetrySnapshot{
			OpenConnections: g.openConns.Load(),
			ActiveTunnels:   g.tunnelsLive.Load(),
			TunnelsOpened:   g.tunnelsHit.Load(),
			TotalRequests:   g.totalReq.Load(),
		},
	}

	_ = json.NewEncoder(w).Encode(payload)
}

func camoNotModified(w http.ResponseWriter, r *http.Request, etag, lastModified string) {
	h := w.Header()
	h.Set("Server", openrestyServerToken)
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	h.Set("Last-Modified", lastModified)
	h.Set("ETag", etag)
	if shouldCloseConnection(r) {
		h.Set("Connection", "close")
	} else {
		h.Set("Connection", "keep-alive")
	}
	w.WriteHeader(http.StatusNotModified)
}

func camoOpenResty(w http.ResponseWriter, r *http.Request, statusCode int, body string, extraHeaders map[string]string) {
	h := w.Header()

	h.Set("Server", openrestyServerToken)
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	h.Set("Content-Type", "text/html")
	h.Set("Content-Length", strconv.Itoa(len(body)))

	if shouldCloseConnection(r) {
		h.Set("Connection", "close")
	} else {
		h.Set("Connection", "keep-alive")
	}

	for k, v := range extraHeaders {
		h.Set(k, v)
	}

	w.WriteHeader(statusCode)

	if r.Method == http.MethodHead {
		return
	}

	_, _ = io.WriteString(w, body)
}

func shouldCloseConnection(r *http.Request) bool {
	if r.Close {
		return true
	}
	for _, v := range r.Header.Values("Connection") {
		for _, tok := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), "close") {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Hijack-Transparent ResponseWriter wrapper & tunnel tracking
// ---------------------------------------------------------------------------

type hijackTrackingWriter struct {
	http.ResponseWriter
	gw *Gateway
}

func (w *hijackTrackingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *hijackTrackingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *hijackTrackingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijack: underlying ResponseWriter does not support Hijack")
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		return nil, nil, err
	}
	tc := &trackedConn{Conn: conn, gw: w.gw}
	tc.gw.registerTunnel(tc)
	return tc, brw, nil
}

type trackedConn struct {
	net.Conn
	gw   *Gateway
	once sync.Once
}

func (c *trackedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.gw.unregisterTunnel(c) })
	return err
}
