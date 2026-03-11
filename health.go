package dbresolver

import (
	"context"
	"sync"
	"time"
)

type nodeHealthState struct {
	healthy         bool
	consecutiveFail int
	consecutiveOK   int
}

// healthChecker runs periodic pings against all registered nodes and
// tracks their health status. Unhealthy nodes are excluded from load
// balancer selection until they recover.
type healthChecker struct {
	interval         time.Duration
	failThreshold    int
	recoverThreshold int

	mu         sync.RWMutex
	nodeHealth map[*Node]*nodeHealthState

	stopCh chan struct{}
	done   chan struct{}
}

func newHealthChecker(interval time.Duration, failThreshold, recoverThreshold int) *healthChecker {
	if failThreshold <= 0 {
		failThreshold = 3
	}
	if recoverThreshold <= 0 {
		recoverThreshold = 2
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &healthChecker{
		interval:         interval,
		failThreshold:    failThreshold,
		recoverThreshold: recoverThreshold,
		nodeHealth:       make(map[*Node]*nodeHealthState),
		stopCh:           make(chan struct{}),
		done:             make(chan struct{}),
	}
}

func (hc *healthChecker) register(nodes []*Node) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for _, n := range nodes {
		if _, exists := hc.nodeHealth[n]; !exists {
			hc.nodeHealth[n] = &nodeHealthState{healthy: true}
		}
	}
}

func (hc *healthChecker) start() {
	go hc.run()
}

func (hc *healthChecker) stop() {
	close(hc.stopCh)
	<-hc.done
}

func (hc *healthChecker) run() {
	defer close(hc.done)
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.stopCh:
			return
		case <-ticker.C:
			hc.checkAll()
		}
	}
}

func (hc *healthChecker) checkAll() {
	hc.mu.RLock()
	nodes := make([]*Node, 0, len(hc.nodeHealth))
	for n := range hc.nodeHealth {
		nodes = append(nodes, n)
	}
	hc.mu.RUnlock()

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			hc.checkNode(n)
		}(node)
	}
	wg.Wait()
}

func (hc *healthChecker) checkNode(node *Node) {
	timeout := hc.interval / 2
	if timeout < time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := node.db.PingContext(ctx)

	hc.mu.Lock()
	defer hc.mu.Unlock()

	state, ok := hc.nodeHealth[node]
	if !ok {
		return
	}

	if err != nil {
		state.consecutiveOK = 0
		state.consecutiveFail++
		if state.healthy && state.consecutiveFail >= hc.failThreshold {
			state.healthy = false
		}
	} else {
		state.consecutiveFail = 0
		state.consecutiveOK++
		if !state.healthy && state.consecutiveOK >= hc.recoverThreshold {
			state.healthy = true
		}
	}
}

func (hc *healthChecker) isHealthy(node *Node) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	state, ok := hc.nodeHealth[node]
	if !ok {
		return true
	}
	return state.healthy
}
