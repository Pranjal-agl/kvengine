package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestMembershipChaos interleaves random membership changes, partitions,
// crashes and restarts (with on-disk state) with a stream of proposals,
// while a monitor samples every node's role and term. It checks Raft's two
// headline safety properties:
//
//   - Election Safety: never two leaders in the same term.
//   - State Machine Safety: every node applies the same commands in the
//     same order (each node's applied list is a prefix of the longest),
//     and no command twice.
func TestMembershipChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { chaosRun(t, seed) })
	}
}

func chaosRun(t *testing.T, seed int64) {
	r := rand.New(rand.NewSource(seed))
	c := newMCluster(t, 3, t.TempDir())
	all := []string{"m0", "m1", "m2"}
	down := map[string]bool{}
	next := 3

	// Election Safety monitor.
	stop := make(chan struct{})
	violation := make(chan string, 1)
	go func() {
		leaders := map[uint64]string{}
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.mu.Lock()
			for id, n := range c.nodes {
				if s := n.State(); s.Role == Leader {
					if prev, ok := leaders[s.Term]; ok && prev != id {
						select {
						case violation <- fmt.Sprintf("term %d has two leaders: %s and %s", s.Term, prev, id):
						default:
						}
					}
					leaders[s.Term] = id
				}
			}
			c.mu.Unlock()
			time.Sleep(200 * time.Microsecond)
		}
	}()

	committed, attempts := 0, 0
	tryPropose := func() {
		c.mu.Lock()
		var l *Node
		for id, n := range c.nodes {
			if !down[id] && n.State().Role == Leader {
				l = n
			}
		}
		c.mu.Unlock()
		if l == nil {
			return
		}
		// Unique per attempt: a timed-out proposal may still commit later.
		cmd := fmt.Sprintf("c%d", attempts)
		attempts++
		if idx, term, ok := l.Propose([]byte(cmd)); ok && l.WaitCommitted(idx, term, 300*time.Millisecond) == nil {
			committed++
		}
	}
	tryChange := func(f func(*Node) (uint64, uint64, error)) {
		c.mu.Lock()
		var l *Node
		for id, n := range c.nodes {
			if !down[id] && n.State().Role == Leader {
				l = n
			}
		}
		c.mu.Unlock()
		if l != nil {
			if idx, term, err := f(l); err == nil {
				l.WaitCommitted(idx, term, 500*time.Millisecond)
			}
		}
	}

	transfers := 0
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		// Pace the chaos so nodes live long enough to hold elections
		// (timeouts are 200-400ms); otherwise nothing ever commits and
		// the safety checks pass vacuously.
		time.Sleep(time.Duration(10+r.Intn(30)) * time.Millisecond)
		switch r.Intn(11) {
		case 0, 1, 2, 3:
			tryPropose()
		case 4: // partition a random pair
			a, b := all[r.Intn(len(all))], all[r.Intn(len(all))]
			c.tr.Partition(a, b)
			c.tr.Partition(b, a)
		case 5:
			c.tr.HealAll()
		case 6: // add a brand-new node
			if len(all) >= 12 {
				break
			}
			id := fmt.Sprintf("m%d", next)
			next++
			c.start(id, Membership{})
			all = append(all, id)
			tryChange(func(l *Node) (uint64, uint64, error) { return l.AddServerTimeout(id, "", 500*time.Millisecond) })
		case 7: // remove a random member, keeping at least 3
			c.mu.Lock()
			var l *Node
			for id, n := range c.nodes {
				if !down[id] && n.State().Role == Leader {
					l = n
				}
			}
			c.mu.Unlock()
			if l != nil {
				if ids := l.Members().IDs(); len(ids) > 3 {
					victim := ids[r.Intn(len(ids))]
					tryChange(func(l *Node) (uint64, uint64, error) { return l.RemoveServer(victim) })
				}
			}
		case 8: // crash a node (at most two down at once)
			id := all[r.Intn(len(all))]
			if !down[id] && len(down) < 2 {
				c.stop(id)
				down[id] = true
			}
		case 10: // hand leadership to a random member
			c.mu.Lock()
			var l *Node
			for id, n := range c.nodes {
				if !down[id] && n.State().Role == Leader {
					l = n
				}
			}
			c.mu.Unlock()
			if l != nil {
				ids := l.Members().IDs()
				l.TransferLeadership(ids[r.Intn(len(ids))], 300*time.Millisecond)
				transfers++
			}
		case 9: // restart a crashed node from its files
			for id := range down {
				c.start(id, Membership{"m0": "", "m1": "", "m2": ""})
				delete(down, id)
				break
			}
		}
		select {
		case v := <-violation:
			t.Fatal(v)
		default:
		}
	}

	// Heal everything, restart everyone, and let the cluster converge.
	c.tr.HealAll()
	for id := range down {
		c.start(id, Membership{"m0": "", "m1": "", "m2": ""})
		delete(down, id)
	}
	duringChaos := committed
	for end := time.Now().Add(5 * time.Second); committed == duringChaos && time.Now().Before(end); {
		tryPropose()
		time.Sleep(20 * time.Millisecond)
	}
	before := duringChaos
	close(stop)
	select {
	case v := <-violation:
		t.Fatal(v)
	default:
	}
	if duringChaos == 0 {
		t.Fatal("nothing committed during the chaos phase; the test is not exercising Raft")
	}
	if committed == before {
		c.mu.Lock()
		for id, n := range c.nodes {
			n.mu.Lock()
			t.Logf("%s: role=%v term=%d leader=%q members=%v cfgIdx=%d commit=%d last=%d voted=%q contactAgo=%v stopped=%v",
				id, n.role, n.currentTerm, n.leaderId, n.config, n.configIndex, n.commitIndex, n.lastIndex(), n.votedFor,
				time.Since(n.lastLeaderContact).Round(time.Millisecond), n.stopped())
			n.mu.Unlock()
		}
		c.mu.Unlock()
		t.Fatal("cluster made no progress after healing")
	}

	// State Machine Safety across every node that ever ran.
	time.Sleep(300 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	var longest []string
	for _, cmds := range c.applied {
		if len(cmds) > len(longest) {
			longest = cmds
		}
	}
	for id, cmds := range c.applied {
		seen := map[string]bool{}
		for i, cmd := range cmds {
			if seen[cmd] {
				t.Fatalf("%s applied %q twice", id, cmd)
			}
			seen[cmd] = true
			if cmd != longest[i] {
				t.Fatalf("%s diverges at %d: %q vs %q", id, i, cmd, longest[i])
			}
		}
	}
	t.Logf("%d commits (%d under chaos), %d nodes ever, %d transfers attempted, final leader members %v",
		committed, duringChaos, len(c.applied), transfers, func() string {
			for _, n := range c.nodes {
				if n.State().Role == Leader {
					return n.Members().String()
				}
			}
			return "?"
		}())
}
