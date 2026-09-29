package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// BERMUDA Stealth Gateway NG — Resilient Supervisor Daemon
// Process Lifecycle, POSIX Termination & Adaptive Concurrent Probing
// ---------------------------------------------------------------------------

const (
	defaultXrayBin    = "/usr/local/bin/xray"
	defaultConfigPath = "/app/config.json"
	defaultAssetDir   = "/usr/local/share/xray"
	defaultXrayMemMB  = 640

	defaultLoopbackXH = "127.0.0.1:18443"
	defaultLoopbackWS = "127.0.0.1:18444"
	defaultLoopbackTR = "127.0.0.1:18445"

	fastProbeWindow     = 1500 * time.Millisecond
	fastProbeInterval   = 25 * time.Millisecond
	slowProbeInterval   = 100 * time.Millisecond
	probeDialTimeout    = 250 * time.Millisecond
	defaultStartTimeout = 10 * time.Second
	defaultStopTimeout  = 8 * time.Second
	groupPollInterval   = 20 * time.Millisecond
	pipeReadBufferSize  = 64 * 1024

	stableWindow       = 60 * time.Second
	initialBackoff     = 80 * time.Millisecond
	maxBackoffInterval = 5 * time.Second
	backoffJitterFrac  = 0.20

	healthGenShift = 8
)

const (
	healthRunning uint64 = 1 << iota
	healthReady
	healthXH
	healthWS
	healthTR
)

// scannerBufPool provides pooled, reusable bufio.Reader instances.
// ReadSlice returns bounded fragments on ErrBufferFull, enabling uncrashable
// streaming across arbitrarily long log records without unbounded heap allocation.
var scannerBufPool = sync.Pool{
	New: func() any { return bufio.NewReaderSize(nil, pipeReadBufferSize) },
}

var pumpLogMu sync.Mutex
var pumpNewline = [1]byte{'\n'}
var subreaperOnce sync.Once
var subreaperErr error

// SupervisorHealthSnapshot captures a consistent, point-in-time health telemetry view.
type SupervisorHealthSnapshot struct {
	Running     bool   `json:"running"`
	Ready       bool   `json:"ready"`
	Restarts    int32  `json:"restarts"`
	UptimeSec   int64  `json:"uptime_sec"`
	XHInbound   bool   `json:"xh_inbound_ok"`
	WSInbound   bool   `json:"ws_inbound_ok"`
	TRInbound   bool   `json:"tr_inbound_ok"`
	LastProbeAt string `json:"last_probe_at"`
	ChildPID    int    `json:"child_pid,omitempty"`
}

// Supervisor manages the lifecycle, execution, telemetry, and graceful teardown
// of the Xray-core child daemon process. Health flags and child generations
// are packed in one atomic word to prevent stale probe commits across restarts.
type Supervisor struct {
	binPath      string
	configPath   string
	assetDir     string
	memLimitMB   int
	xhAddr       string
	wsAddr       string
	trAddr       string
	startTimeout time.Duration
	stopTimeout  time.Duration

	mu         sync.Mutex
	cmd        *exec.Cmd
	startedAt  time.Time
	lastUptime time.Duration
	runCancel  context.CancelFunc

	runStarted    atomic.Bool
	stopRequested atomic.Bool
	restarts      atomic.Int32
	stopGraceNS   atomic.Int64
	runReady      chan struct{}
	stopped       chan struct{}

	// state = (generation << healthGenShift) | health flags
	state       atomic.Uint64
	lastProbeNS atomic.Int64

	probeMu     sync.Mutex
	probeDialer *net.Dialer
}

// NewSupervisor initializes the supervisor with hardened defaults and zero-wait probe dialers.
func NewSupervisor() *Supervisor {
	// Enable PR_SET_CHILD_SUBREAPER so the gateway adopts and cleans up any
	// orphaned worker processes spawned by child daemons.
	subreaperOnce.Do(func() {
		_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 36, 1, 0, 0, 0, 0)
		if errno != 0 {
			subreaperErr = errno
		}
	})
	if subreaperErr != nil {
		log.Printf("[Supervisor] Warning: cannot enable child-subreaper mode: %v", subreaperErr)
	}

	bin := getEnv("BERMUDA_XRAY_BIN", defaultXrayBin)
	cfg := getEnv("BERMUDA_XRAY_CONFIG", defaultConfigPath)
	assets := getEnv("XRAY_LOCATION_ASSET", defaultAssetDir)

	dialer := &net.Dialer{
		Timeout:   probeDialTimeout,
		KeepAlive: -1,
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				controlErr = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET,
					syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 0})
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}

	s := &Supervisor{
		binPath:      bin,
		configPath:   cfg,
		assetDir:     assets,
		memLimitMB:   defaultXrayMemMB,
		xhAddr:       getEnv("BERMUDA_BACKEND_XH", defaultLoopbackXH),
		wsAddr:       getEnv("BERMUDA_BACKEND_WS", defaultLoopbackWS),
		trAddr:       getEnv("BERMUDA_BACKEND_TR", defaultLoopbackTR),
		startTimeout: defaultStartTimeout,
		stopTimeout:  defaultStopTimeout,
		runReady:     make(chan struct{}),
		stopped:      make(chan struct{}),
		probeDialer:  dialer,
	}
	s.stopGraceNS.Store(int64(defaultStopTimeout))
	return s
}

func (s *Supervisor) IsRunning() bool             { return s.state.Load()&healthRunning != 0 }
func (s *Supervisor) IsReady() bool               { return s.state.Load()&healthReady != 0 }
func (s *Supervisor) Restarts() int32             { return s.restarts.Load() }
func (s *Supervisor) RunStarted() <-chan struct{} { return s.runReady }

// Preflight executes a dry-run configuration syntax test before spawning.
func (s *Supervisor) Preflight() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.PreflightContext(ctx)
}

// PreflightContext executes syntax validation with parent context awareness.
func (s *Supervisor) PreflightContext(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.binPath, "run", "-test", "-c", s.configPath)
	cmd.Env = replaceEnv(os.Environ(), map[string]string{"XRAY_LOCATION_ASSET": s.assetDir})
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		terminateProcessGroup(cmd.Process.Pid, 200*time.Millisecond)
		return nil
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("xray preflight validation failed: %w, output: %s", err, strings.TrimSpace(string(out)))
	}
	log.Printf("[Supervisor] Preflight validation passed for %s", s.configPath)
	return nil
}

// Run executes the continuous supervision loop until parent context is canceled.
func (s *Supervisor) Run(parent context.Context) error {
	s.mu.Lock()
	if !s.runStarted.CompareAndSwap(false, true) {
		s.mu.Unlock()
		return errors.New("supervisor Run may only be called once")
	}
	runCtx, cancel := context.WithCancel(parent)
	s.runCancel = cancel
	close(s.runReady)
	s.mu.Unlock()

	defer func() {
		cancel()
		s.mu.Lock()
		s.runCancel = nil
		s.mu.Unlock()
		close(s.stopped)
	}()

	backoff := initialBackoff
	for {
		if s.stopRequested.Load() || runCtx.Err() != nil {
			return nil
		}

		err := s.startAndWait(runCtx)
		if s.stopRequested.Load() || runCtx.Err() != nil {
			return nil
		}

		s.restarts.Add(1)
		s.mu.Lock()
		uptime := s.lastUptime
		s.mu.Unlock()
		if uptime >= stableWindow {
			backoff = initialBackoff
		}
		if err == nil {
			err = errors.New("Xray exited normally")
		}

		delay := jitteredBackoff(backoff)
		log.Printf("[Supervisor] Xray stopped after %s (err: %v); restart=%d in %s",
			uptime.Round(time.Millisecond), err, s.restarts.Load(), delay.Round(time.Millisecond))

		timer := time.NewTimer(delay)
		select {
		case <-runCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maxBackoffInterval {
			backoff = maxBackoffInterval
		}
	}
}

// jitteredBackoff applies uniform ±20% jitter using lock-free math/rand/v2.
func jitteredBackoff(base time.Duration) time.Duration {
	if base <= 0 {
		return base
	}
	spread := float64(base) * backoffJitterFrac
	delta := (rand.Float64()*2 - 1) * spread
	result := time.Duration(float64(base) + delta)
	if result < 0 {
		return 0
	}
	return result
}

func (s *Supervisor) startAndWait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.lastUptime = 0
	s.mu.Unlock()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create stdout pipe: %w", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		return fmt.Errorf("create stderr pipe: %w", err)
	}

	cmd := exec.Command(s.binPath, "run", "-c", s.configPath)
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	cmd.Env = replaceEnv(os.Environ(), map[string]string{
		"XRAY_LOCATION_ASSET": s.assetDir,
		"GOMEMLIMIT":          fmt.Sprintf("%dMiB", s.memLimitMB),
		"GODEBUG":             mergeCSVEnv(os.Getenv("GODEBUG"), "madvdontneed=1"),
		"GOGC":                "100",
	})
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}

	if err := cmd.Start(); err != nil {
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		_ = stderrR.Close()
		_ = stderrW.Close()
		return fmt.Errorf("start Xray: %w", err)
	}

	// Close parent write ends immediately; ensures readers observe EOF on child exit.
	_ = stdoutW.Close()
	_ = stderrW.Close()

	generation := s.startGeneration()
	s.mu.Lock()
	s.cmd = cmd
	s.startedAt = time.Now()
	s.lastUptime = 0
	s.mu.Unlock()
	pid := cmd.Process.Pid
	log.Printf("[Supervisor] Xray started pid=%d pgid=%d GOMEMLIMIT=%dMiB (runtime soft target)", pid, pid, s.memLimitMB)

	var pumpWG sync.WaitGroup
	pumpWG.Add(2)
	go func() { defer pumpWG.Done(); s.pumpPipe(stdoutR, "[Xray-Out]") }()
	go func() { defer pumpWG.Done(); s.pumpPipe(stderrR, "[Xray-Err]") }()
	pumpDone := make(chan struct{})
	go func() { pumpWG.Wait(); close(pumpDone) }()

	probeCtx, cancelProbes := context.WithCancel(ctx)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		s.awaitReadiness(probeCtx, generation)
	}()
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		s.reapOrphanedChildren(probeCtx, pid)
	}()

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-waitCh:
		terminateProcessGroup(pid, 200*time.Millisecond)
	case <-ctx.Done():
		terminateProcessGroup(pid, s.terminationGrace())
		waitErr = <-waitCh // Direct child reaped structurally
	}

	cancelProbes()
	<-probeDone
	<-reaperDone
	s.endGeneration(generation)
	s.mu.Lock()
	s.lastUptime = time.Since(s.startedAt)
	if s.cmd == cmd {
		s.cmd = nil
	}
	s.mu.Unlock()

	// Clean up any adopted descendants
	cleanupAdoptedChildren(200 * time.Millisecond)
	reapProcessGroup(pid)

	select {
	case <-pumpDone:
	case <-time.After(1500 * time.Millisecond):
		_ = stdoutR.Close()
		_ = stderrR.Close()
		select {
		case <-pumpDone:
		case <-time.After(250 * time.Millisecond):
			log.Printf("[Supervisor] Warning: log pump did not stop promptly after process-group teardown")
		}
	}
	_ = stdoutR.Close()
	_ = stderrR.Close()

	if ctx.Err() != nil || s.stopRequested.Load() {
		log.Printf("[Supervisor] Xray process group pid=%d stopped and direct child reaped", pid)
		return nil
	}
	if waitErr != nil {
		return fmt.Errorf("Xray pid=%d exited: %w", pid, waitErr)
	}
	return fmt.Errorf("Xray pid=%d exited with status 0", pid)
}

func (s *Supervisor) startGeneration() uint64 {
	for {
		old := s.state.Load()
		gen := (old >> healthGenShift) + 1
		next := (gen << healthGenShift) | healthRunning
		if s.state.CompareAndSwap(old, next) {
			return gen
		}
	}
}

func (s *Supervisor) endGeneration(generation uint64) {
	for {
		old := s.state.Load()
		if old>>healthGenShift != generation {
			return
		}
		next := (generation + 1) << healthGenShift
		if s.state.CompareAndSwap(old, next) {
			return
		}
	}
}

func (s *Supervisor) commitProbe(generation uint64, xh, ws, tr bool) bool {
	var flags uint64 = healthRunning
	if xh {
		flags |= healthXH
	}
	if ws {
		flags |= healthWS
	}
	if tr {
		flags |= healthTR
	}
	if xh && ws && tr {
		flags |= healthReady
	}
	for {
		old := s.state.Load()
		if old>>healthGenShift != generation || old&healthRunning == 0 {
			return false
		}
		if s.state.CompareAndSwap(old, (generation<<healthGenShift)|flags) {
			s.lastProbeNS.Store(time.Now().UnixNano())
			return true
		}
	}
}

// pumpPipe streams arbitrarily long records through a pooled fixed-size reader.
// ReadSlice reports ErrBufferFull on oversized lines, emitting bounded pieces
// without crashing or stopping. Invalid UTF-8 bytes are written verbatim.
func (s *Supervisor) pumpPipe(r io.Reader, prefix string) {
	reader := scannerBufPool.Get().(*bufio.Reader)
	reader.Reset(r)
	defer func() {
		reader.Reset(nil)
		scannerBufPool.Put(reader)
	}()

	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) != 0 {
			writePumpRecord(os.Stderr, prefix, fragment)
		}
		if err == nil || errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return
		}
		writePumpRecord(os.Stderr, prefix, []byte("log pipe read error: "+err.Error()))
		return
	}
}

// writePumpRecord formats UTC timestamps and emits raw bytes without string conversion.
func writePumpRecord(dst *os.File, prefix string, data []byte) {
	pumpLogMu.Lock()
	defer pumpLogMu.Unlock()

	now := time.Now().UTC()
	var header [256]byte
	year := now.Year()
	header[0] = byte('0' + year/1000%10)
	header[1] = byte('0' + year/100%10)
	header[2] = byte('0' + year/10%10)
	header[3] = byte('0' + year%10)
	header[4] = '/'
	putTwoDigits(header[5:7], int(now.Month()))
	header[7] = '/'
	putTwoDigits(header[8:10], now.Day())
	header[10] = ' '
	putTwoDigits(header[11:13], now.Hour())
	header[13] = ':'
	putTwoDigits(header[14:16], now.Minute())
	header[16] = ':'
	putTwoDigits(header[17:19], now.Second())
	header[19] = ' '
	pos := 20 + copy(header[20:len(header)-1], prefix)
	header[pos] = ' '
	writeFileAll(dst, header[:pos+1])
	writeFileAll(dst, data)
	if len(data) == 0 || data[len(data)-1] != '\n' {
		writeFileAll(dst, pumpNewline[:])
	}
}

func putTwoDigits(dst []byte, value int) {
	dst[0] = byte('0' + value/10%10)
	dst[1] = byte('0' + value%10)
}

func writeFileAll(dst *os.File, data []byte) {
	for len(data) != 0 {
		n, err := dst.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil || n == 0 {
			return
		}
	}
}

func (s *Supervisor) awaitReadiness(ctx context.Context, generation uint64) {
	started := time.Now()
	deadline := started.Add(s.startTimeout)
	var warned bool
	nextDelay := time.Duration(0) // Probe immediately on boot

	for {
		state := s.state.Load()
		if state>>healthGenShift != generation || state&healthRunning == 0 {
			return
		}
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			return
		}

		prior := s.state.Load()
		xh, ws, tr := s.probeAll(ctx, generation)
		if s.commitProbe(generation, xh, ws, tr) && xh && ws && tr && prior&healthReady == 0 {
			log.Printf("[Supervisor] All loopback inbounds ready after %s", time.Since(started).Round(time.Millisecond))
		}
		if !warned && time.Now().After(deadline) {
			warned = true
			log.Printf("[Supervisor] Readiness has not converged after %s; probes will continue while Xray runs", s.startTimeout)
		}
		if time.Since(started) < fastProbeWindow {
			nextDelay = fastProbeInterval
		} else {
			nextDelay = slowProbeInterval
		}
	}
}

func (s *Supervisor) probeAll(ctx context.Context, generation uint64) (bool, bool, bool) {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	state := s.state.Load()
	if ctx.Err() != nil || state>>healthGenShift != generation || state&healthRunning == 0 {
		return false, false, false
	}

	// Run all three loopback dials concurrently
	results := make(chan struct {
		index int
		ok    bool
	}, 3)
	addresses := [3]string{s.xhAddr, s.wsAddr, s.trAddr}
	for i, address := range addresses {
		go func(index int, addr string) {
			results <- struct {
				index int
				ok    bool
			}{index: index, ok: s.probeTCP(ctx, addr)}
		}(i, address)
	}
	var status [3]bool
	for range addresses {
		select {
		case result := <-results:
			status[result.index] = result.ok
		case <-ctx.Done():
			return false, false, false
		}
	}
	return status[0], status[1], status[2]
}

func (s *Supervisor) probeTCP(parent context.Context, addr string) bool {
	ctx, cancel := context.WithTimeout(parent, probeDialTimeout)
	defer cancel()
	conn, err := s.probeDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close() // SO_LINGER {1,0} immediate RST abort, zero TIME_WAIT
	return true
}

// ProbeNow executes an active on-demand health check across all 3 inbounds for /healthz.
func (s *Supervisor) ProbeNow() (bool, bool, bool) {
	state := s.state.Load()
	if state&healthRunning == 0 {
		return false, false, false
	}
	generation := state >> healthGenShift
	xh, ws, tr := s.probeAll(context.Background(), generation)
	if !s.commitProbe(generation, xh, ws, tr) {
		return false, false, false
	}
	return xh, ws, tr
}

// Snapshot gathers instantaneous health telemetry atomically across all inbounds.
func (s *Supervisor) Snapshot() SupervisorHealthSnapshot {
	state := s.state.Load()
	s.mu.Lock()
	started := s.startedAt
	pid := 0
	if s.cmd != nil && s.cmd.Process != nil {
		pid = s.cmd.Process.Pid
	}
	s.mu.Unlock()

	var uptime int64
	if state&healthRunning != 0 && !started.IsZero() {
		uptime = int64(time.Since(started).Seconds())
	}
	lastProbeAt := ""
	if probedAt := s.lastProbeNS.Load(); probedAt != 0 {
		lastProbeAt = time.Unix(0, probedAt).UTC().Format(time.RFC3339Nano)
	}
	return SupervisorHealthSnapshot{
		Running:     state&healthRunning != 0,
		Ready:       state&healthReady != 0,
		Restarts:    s.restarts.Load(),
		UptimeSec:   uptime,
		XHInbound:   state&healthXH != 0,
		WSInbound:   state&healthWS != 0,
		TRInbound:   state&healthTR != 0,
		LastProbeAt: lastProbeAt,
		ChildPID:    pid,
	}
}

// Stop cancels supervision, terminates the process group, reaps zombies, and blocks until finished.
func (s *Supervisor) Stop(grace time.Duration) {
	if grace <= 0 {
		grace = s.stopTimeout
	}
	s.stopGraceNS.Store(int64(grace))
	s.stopRequested.Store(true)
	s.mu.Lock()
	cancel := s.runCancel
	running := s.runStarted.Load()
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if running {
		<-s.stopped
	}
}

func (s *Supervisor) terminationGrace() time.Duration {
	n := s.stopGraceNS.Load()
	if n <= 0 {
		return s.stopTimeout
	}
	return time.Duration(n)
}

func terminateProcessGroup(pgid int, grace time.Duration) {
	if pgid <= 1 {
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Printf("[Supervisor] SIGTERM process group %d: %v", pgid, err)
	}
	deadline := time.Now().Add(grace)
	ticker := time.NewTicker(groupPollInterval)
	defer ticker.Stop()
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if !time.Now().Before(deadline) {
			log.Printf("[Supervisor] Process group %d exceeded TERM grace; sending SIGKILL", pgid)
			if killErr := syscall.Kill(-pgid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
				log.Printf("[Supervisor] SIGKILL process group %d: %v", pgid, killErr)
			}
			return
		}
		<-ticker.C
	}
}

func reapProcessGroup(pgid int) {
	if pgid <= 1 {
		return
	}
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-pgid, &status, syscall.WNOHANG, nil)
		if pid > 0 {
			continue
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil && !errors.Is(err, syscall.ECHILD) {
			log.Printf("[Supervisor] wait4 process group %d: %v", pgid, err)
		}
		return
	}
}

// reapOrphanedChildren periodically reaps adopted children while Xray is running.
func (s *Supervisor) reapOrphanedChildren(ctx context.Context, protectedPID int) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		children, err := directChildPIDs()
		if err != nil {
			log.Printf("[Supervisor] Warning: cannot enumerate adopted children: %v", err)
			return
		}
		for _, pid := range children {
			if pid == protectedPID {
				continue
			}
			var status syscall.WaitStatus
			if _, waitErr := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); waitErr != nil &&
				!errors.Is(waitErr, syscall.ECHILD) && !errors.Is(waitErr, syscall.EINTR) {
				log.Printf("[Supervisor] wait4 adopted child %d: %v", pid, waitErr)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// cleanupAdoptedChildren terminates and reaps any workers that escaped session groups.
func cleanupAdoptedChildren(grace time.Duration) {
	deadline := time.Now().Add(grace)
	forceKill := false
	for {
		reapAdoptedChildren()
		children, err := directChildPIDs()
		if err != nil {
			log.Printf("[Supervisor] Warning: cannot enumerate adopted children: %v", err)
			return
		}
		if len(children) == 0 {
			return
		}
		for _, pid := range children {
			signal := syscall.SIGTERM
			if forceKill {
				signal = syscall.SIGKILL
			}
			if killErr := syscall.Kill(pid, signal); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
				log.Printf("[Supervisor] signal adopted child %d: %v", pid, killErr)
			}
		}
		if !forceKill && !time.Now().Before(deadline) {
			forceKill = true
		}
		time.Sleep(groupPollInterval)
	}
}

func reapAdoptedChildren() {
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if pid > 0 {
			continue
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil && !errors.Is(err, syscall.ECHILD) {
			log.Printf("[Supervisor] wait4 adopted children: %v", err)
		}
		return
	}
}

func directChildPIDs() ([]int, error) {
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, err
	}
	seen := make(map[int]struct{})
	for _, task := range tasks {
		data, readErr := os.ReadFile("/proc/self/task/" + task.Name() + "/children")
		if readErr != nil {
			continue
		}
		for _, field := range strings.Fields(string(data)) {
			pid, parseErr := strconv.Atoi(field)
			if parseErr == nil && pid > 1 {
				seen[pid] = struct{}{}
			}
		}
	}
	children := make([]int, 0, len(seen))
	for pid := range seen {
		children = append(children, pid)
	}
	return children, nil
}

func replaceEnv(env []string, replacements map[string]string) []string {
	out := make([]string, 0, len(env)+len(replacements))
	seen := make(map[string]struct{}, len(replacements))
	for _, item := range env {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			out = append(out, item)
			continue
		}
		if value, replace := replacements[key]; replace {
			if _, already := seen[key]; !already {
				out = append(out, key+"="+value)
				seen[key] = struct{}{}
			}
			continue
		}
		out = append(out, item)
	}
	for key, value := range replacements {
		if _, ok := seen[key]; !ok {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func mergeCSVEnv(existing, required string) string {
	key, _, _ := strings.Cut(required, "=")
	parts := strings.Split(existing, ",")
	kept := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		partKey, _, _ := strings.Cut(part, "=")
		if partKey != key && part != "" {
			kept = append(kept, part)
		}
	}
	kept = append(kept, required)
	return strings.Join(kept, ",")
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
