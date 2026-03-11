package dbresolver

import (
	"sync"
	"testing"
)

func makeNodes(n int) []*Node {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = &Node{weight: 1}
	}
	return nodes
}

func makeWeightedNodes(weights []int) []*Node {
	nodes := make([]*Node, len(weights))
	for i, w := range weights {
		nodes[i] = &Node{weight: w}
	}
	return nodes
}

func TestRoundRobinLB(t *testing.T) {
	lb := NewRoundRobinLB()
	nodes := makeNodes(3)

	for cycle := 0; cycle < 3; cycle++ {
		for i := 0; i < 3; i++ {
			got := lb.Pick(nodes)
			if got != nodes[i] {
				t.Errorf("cycle %d, pick %d: got node %p, want %p", cycle, i, got, nodes[i])
			}
		}
	}
}

func TestRoundRobinLB_SingleNode(t *testing.T) {
	lb := NewRoundRobinLB()
	nodes := makeNodes(1)

	for i := 0; i < 5; i++ {
		got := lb.Pick(nodes)
		if got != nodes[0] {
			t.Error("should always return the single node")
		}
	}
}

func TestRoundRobinLB_Empty(t *testing.T) {
	lb := NewRoundRobinLB()
	if got := lb.Pick(nil); got != nil {
		t.Error("should return nil for empty nodes")
	}
}

func TestRoundRobinLB_Concurrent(t *testing.T) {
	lb := NewRoundRobinLB()
	nodes := makeNodes(3)
	counts := make([]int, 3)
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := lb.Pick(nodes)
			mu.Lock()
			for j, n := range nodes {
				if n == node {
					counts[j]++
					break
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	for i, c := range counts {
		if c != 100 {
			t.Errorf("node %d: got %d picks, want 100", i, c)
		}
	}
}

func TestRandomLB(t *testing.T) {
	lb := NewRandomLB()
	nodes := makeNodes(3)

	picked := make(map[*Node]bool)
	for i := 0; i < 100; i++ {
		picked[lb.Pick(nodes)] = true
	}

	if len(picked) < 2 {
		t.Error("random LB should pick multiple different nodes over 100 calls")
	}
}

func TestRandomLB_Empty(t *testing.T) {
	lb := NewRandomLB()
	if got := lb.Pick(nil); got != nil {
		t.Error("should return nil for empty nodes")
	}
}

func TestWeightedRoundRobinLB(t *testing.T) {
	lb := NewWeightedRoundRobinLB()
	nodes := makeWeightedNodes([]int{5, 1})

	counts := map[*Node]int{}
	total := 60
	for i := 0; i < total; i++ {
		n := lb.Pick(nodes)
		counts[n]++
	}

	// With weights 5:1, we expect roughly 50:10 distribution
	ratio := float64(counts[nodes[0]]) / float64(counts[nodes[1]])
	if ratio < 4.0 || ratio > 6.0 {
		t.Errorf("expected ~5:1 ratio, got %.1f:1 (counts: %d, %d)",
			ratio, counts[nodes[0]], counts[nodes[1]])
	}
}

func TestWeightedRoundRobinLB_EqualWeights(t *testing.T) {
	lb := NewWeightedRoundRobinLB()
	nodes := makeWeightedNodes([]int{1, 1, 1})

	counts := map[*Node]int{}
	for i := 0; i < 30; i++ {
		counts[lb.Pick(nodes)]++
	}

	for i, n := range nodes {
		if counts[n] != 10 {
			t.Errorf("node %d: got %d, want 10 (equal weights)", i, counts[n])
		}
	}
}

func TestWeightedRoundRobinLB_ZeroWeight(t *testing.T) {
	lb := NewWeightedRoundRobinLB()
	nodes := makeWeightedNodes([]int{0, 0})

	// Zero weights should be treated as 1
	counts := map[*Node]int{}
	for i := 0; i < 20; i++ {
		counts[lb.Pick(nodes)]++
	}
	for i, n := range nodes {
		if counts[n] != 10 {
			t.Errorf("node %d: got %d, want 10 (zero treated as 1)", i, counts[n])
		}
	}
}

func TestWeightedRoundRobinLB_SingleNode(t *testing.T) {
	lb := NewWeightedRoundRobinLB()
	nodes := makeWeightedNodes([]int{5})

	for i := 0; i < 10; i++ {
		if got := lb.Pick(nodes); got != nodes[0] {
			t.Error("single node should always be returned")
		}
	}
}

func TestNewLoadBalancer(t *testing.T) {
	tests := []struct {
		policy LoadBalancerPolicy
		typ    string
	}{
		{RoundRobinLB, "*dbresolver.roundRobinLB"},
		{RandomLB, "*dbresolver.randomLB"},
		{WeightedRoundRobinLB, "*dbresolver.weightedRoundRobinLB"},
		{LoadBalancerPolicy(99), "*dbresolver.roundRobinLB"}, // unknown defaults to round robin
	}
	for _, tt := range tests {
		lb := newLoadBalancer(tt.policy)
		if lb == nil {
			t.Errorf("newLoadBalancer(%d) returned nil", tt.policy)
		}
	}
}
