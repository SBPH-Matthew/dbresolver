package dbresolver

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

// LoadBalancerPolicy identifies a built-in load balancing strategy.
type LoadBalancerPolicy int

const (
	// RoundRobinLB distributes requests evenly across nodes in order.
	RoundRobinLB LoadBalancerPolicy = iota

	// RandomLB distributes requests randomly across nodes.
	RandomLB

	// WeightedRoundRobinLB distributes requests based on node weights,
	// useful for cross-region topologies where local nodes should receive
	// more traffic than remote ones.
	WeightedRoundRobinLB
)

// LoadBalancer selects a node from a list of available nodes.
// Implementations must be safe for concurrent use.
type LoadBalancer interface {
	Pick(nodes []*Node) *Node
}

// --- Round Robin ---

type roundRobinLB struct {
	counter atomic.Uint64
}

// NewRoundRobinLB creates a round-robin load balancer that cycles through
// nodes sequentially.
func NewRoundRobinLB() LoadBalancer {
	return &roundRobinLB{}
}

func (lb *roundRobinLB) Pick(nodes []*Node) *Node {
	n := uint64(len(nodes))
	if n == 0 {
		return nil
	}
	idx := lb.counter.Add(1) - 1
	return nodes[idx%n]
}

// --- Random ---

type randomLB struct {
	mu  sync.Mutex
	rng *rand.Rand
}

// NewRandomLB creates a random load balancer with a concurrent-safe RNG.
func NewRandomLB() LoadBalancer {
	return &randomLB{
		rng: rand.New(rand.NewSource(rand.Int63())),
	}
}

func (lb *randomLB) Pick(nodes []*Node) *Node {
	n := len(nodes)
	if n == 0 {
		return nil
	}
	lb.mu.Lock()
	idx := lb.rng.Intn(n)
	lb.mu.Unlock()
	return nodes[idx]
}

// --- Weighted Round Robin (Nginx smooth algorithm) ---

type weightedRoundRobinLB struct {
	mu             sync.Mutex
	currentWeights []int
}

// NewWeightedRoundRobinLB creates a weighted round-robin load balancer that
// distributes traffic proportionally to each node's Weight. This uses the
// smooth weighted round-robin algorithm (as used by Nginx) for even distribution.
func NewWeightedRoundRobinLB() LoadBalancer {
	return &weightedRoundRobinLB{}
}

func (lb *weightedRoundRobinLB) Pick(nodes []*Node) *Node {
	n := len(nodes)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return nodes[0]
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.currentWeights) != n {
		lb.currentWeights = make([]int, n)
	}

	totalWeight := 0
	bestIdx := 0

	for i, node := range nodes {
		w := node.weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w
		lb.currentWeights[i] += w

		if lb.currentWeights[i] > lb.currentWeights[bestIdx] {
			bestIdx = i
		}
	}

	lb.currentWeights[bestIdx] -= totalWeight
	return nodes[bestIdx]
}

func newLoadBalancer(policy LoadBalancerPolicy) LoadBalancer {
	switch policy {
	case RandomLB:
		return NewRandomLB()
	case WeightedRoundRobinLB:
		return NewWeightedRoundRobinLB()
	default:
		return NewRoundRobinLB()
	}
}
