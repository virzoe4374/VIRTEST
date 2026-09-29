package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// BERMUDA Stealth Gateway NG — Master Edge Entrypoint & Runtime Orchestrator
// Unified Architecture: cgroup v1/v2 Auto-Quota, Non-Fatal Preflight, 4-Stage Drain
// ---------------------------------------------------------------------------

const (
	defaultPort            = "8080"
	defaultSelfMemMB       = 128
	defaultGOMAXPROCS      = 2
	httpDrainTimeout       = 10 * time.Second
	supervisorStopWait     = 8 * time.Second
	edgeKeepAlivePeriod    = 15 * time.Second
	drainPropagationWindow = 500 * time.Millisecond
)

// applyMemoryCeiling configures Go's runtime memory target (soft limit).
// GOGC=100 paired with this limit prevents GC death spirals while leaving
// ~256MiB for Linux socket/page buffers and kernel overhead.
func applyMemoryCeiling(selfMB int) {
	debug.SetMemoryLimit(int64(selfMB) << 20)
	debug.SetGCPercent(100)
}

// applyGOMAXPROCS dynamically pins the Go scheduler to the container quota.
// Priority: 1. BERMUDA_GOMAXPROCS env -> 2. cgroup v2/v1 quota -> 3. default 2 vCPU.
func applyGOMAXPROCS() int {
	n := 0
	if v := strings.TrimSpace(os.Getenv("BERMUDA_GOMAXPROCS")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n <= 0 {
		if quota, ok := cgroupCPUQuota(); ok {
			n = int(mathCeil(quota))
			if maxCPU := runtime.NumCPU(); n > maxCPU {
				n = maxCPU
			}
		}
	}
	if n <= 0 {
		n = defaultGOMAXPROCS
	}
	runtime.GOMAXPROCS(n)
	return n
}

// cgroupCPUQuota auto-detects CPU limits from cgroup v2 or v1 hierarchies.
func cgroupCPUQuota() (float64, bool) {
	if rel, ok := cgroupV2Path(); ok {
		base := "/sys/fs/cgroup"
		candidates := []string{base + rel + "/cpu.max", base + "/cpu.max"}
		if rel == "/" || rel == "" {
			candidates = []string{base + "/cpu.max"}
		}
		for _, p := range candidates {
			if data, err := os.ReadFile(p); err == nil {
				fields := strings.Fields(string(data))
				if len(fields) == 2 && fields[0] != "max" {
					quota, err1 := strconv.ParseFloat(fields[0], 64)
					period, err2 := strconv.ParseFloat(fields[1], 64)
					if err1 == nil && err2 == nil && quota > 0 && period > 0 {
						return quota / period, true
					}
				}
				return 0, false
			}
		}
	}

	quotaB, err1 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	periodB, err2 := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err1 == nil && err2 == nil {
		quota, e1 := strconv.ParseFloat(strings.TrimSpace(string(quotaB)), 64)
		period, e2 := strconv.ParseFloat(strings.TrimSpace(string(periodB)), 64)
		if e1 == nil && e2 == nil && quota > 0 && period > 0 {
			return quota / period, true
		}
	}
	return 0, false
}

func cgroupV2Path() (string, bool) {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
			return parts[2], true
		}
	}
	return "", false
}

func mathCeil(v float64) float64 {
	if v <= 0 {
		return 0
	}
	i := float64(int(v))
	if i < v {
		return i + 1
	}
	return i
}

// tcpKeepAliveListener tunes accepted client sockets for low-latency line-rate
// streaming and cellular CGNAT persistence without enabling abortive close.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (net.Conn, error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(edgeKeepAlivePeriod)
	_ = tc.SetNoDelay(true)
	return tc, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.Println("[Gateway] Initializing BERMUDA Stealth Gateway NG...")

	// 1. Enforce memory constraints and pin scheduler to 2 vCPUs
	applyMemoryCeiling(defaultSelfMemMB)
	procs := applyGOMAXPROCS()
	log.Printf("[Runtime] Gateway memory ceiling locked at %dMiB (GOGC=100), GOMAXPROCS=%d", defaultSelfMemMB, procs)

	port := getEnv("PORT", defaultPort)

	// 2. Instantiate and preflight validate Xray daemon supervisor
	// Non-fatal preflight: never crashes if Xray has a startup parsing warning
	sup := NewSupervisor()
	if err := sup.Preflight(); err != nil {
		log.Printf("[Gateway] Warning: Supervisor preflight validation issue: %v. Continuing to start...", err)
	}

	// 3. Instantiate reverse proxy gateway with 64KiB channel buffer pool
	gw := NewGateway(sup)

	// 4. Capture container lifecycle termination signals (SIGTERM from Railway / SIGINT)
	rootCtx, stopRoot := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopRoot()

	// 5. Run supervisor loop in a dedicated background goroutine
	supErrCh := make(chan error, 1)
	go func() {
		supErrCh <- sup.Run(rootCtx)
	}()

	// 6. Configure HTTP edge server with line-rate socket tuning
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 5 * time.Second,  // Protects against Slowloris header trickle attacks
		IdleTimeout:       120 * time.Second, // Matches loopback transport idle timeout
		MaxHeaderBytes:    32 * 1024,
		ConnState:         gw.TrackConnState,
	}

	// Manually bind TCP listener to inject TCP_NODELAY socket options
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[Gateway] Fatal: Failed to bind edge listener :%s: %v", port, err)
	}

	tcpListener := &tcpKeepAliveListener{
		TCPListener: ln.(*net.TCPListener),
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("[Gateway] Edge listener active on :%s (PID %d, GOMAXPROCS=%d, GOMEMLIMIT=%dMiB, TCP_NODELAY=true)",
			port, os.Getpid(), procs, defaultSelfMemMB)
		if err := srv.Serve(tcpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	// 7. Await termination signal or unexpected fatal failures
	select {
	case err := <-serverErrCh:
		log.Printf("[Gateway] Fatal: HTTP server failure: %v", err)
		gw.SetDraining()
		gw.CloseTunnels()
		sup.Stop(supervisorStopWait)
		os.Exit(1)

	case err := <-supErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[Gateway] Fatal: Supervisor loop halted unexpectedly: %v", err)
			gw.SetDraining()
			gw.CloseTunnels()
			sup.Stop(supervisorStopWait)
			os.Exit(1)
		}

	case <-rootCtx.Done():
		log.Println("[Gateway] Termination signal intercepted. Commencing graceful drain sequence...")
	}

	// ---------------------------------------------------------------------------
	// Graceful Drain State Machine (Ordered Zero-Downtime Teardown)
	// ---------------------------------------------------------------------------

	// Stage 1: Immediately flip /healthz to 503 so Railway edge mesh sheds traffic
	gw.SetDraining()
	log.Println("[Gateway] Stage 1/4: /healthz flipped to 503 (traffic shedding)")
	time.Sleep(drainPropagationWindow)

	// Stage 2: Allow in-flight connections to drain cleanly up to httpDrainTimeout
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), httpDrainTimeout)
	defer cancelDrain()

	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("[Gateway] Stage 2/4 Warning: HTTP server drain timeout exceeded: %v. Forcing socket closure.", err)
		_ = srv.Close()
	} else {
		log.Println("[Gateway] Stage 2/4: All edge HTTP connections drained successfully.")
	}

	// Stage 3: Force-close active hijacked WebSocket tunnels and release idle loopback sockets
	log.Println("[Gateway] Stage 3/4: Closing hijacked tunnels & releasing idle loopback sockets...")
	gw.CloseTunnels()
	gw.CloseIdleBackendConns()

	// Stage 4: Teardown child Xray process group cleanly and reap zombie state
	log.Println("[Gateway] Stage 4/4: Teardown child Xray process group...")
	sup.Stop(supervisorStopWait)

	log.Println("[Gateway] BERMUDA Stealth Gateway shutdown complete. Ports released cleanly. Exit 0.")
}
