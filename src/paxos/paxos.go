package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	agreeIns      map[int]interface{}
	highestPre    int
	highestAccseq int
	highestAccval interface{}
	prepareokNum  int
	majorityDone  chan bool
	proposers     map[int]Proposer
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.highestAccval = nil
	px.highestAccseq = -1
	px.highestPre = -1
	px.proposers[seq]
	go pr.sendValue(seq, v)
}

type Proposal struct {
	Seq int
	V   interface{}
}

type Proposer struct {
	seq           int
	resProposal   []Proposal
	px            *Paxos
	v             interface{}
	mu            sync.Mutex
	highestAccseq int
	highestPrenum int
	highestAccval interface{}
	majority      int
	// we share the same channel value for accept and prepare
	doneNum    int
	successNum int
	done       chan bool
	reject     chan int
	decided    bool
	peerNum    int
	isreject   bool
}

func (px *Paxos) MakeProposer(v interface{}) *Proposer {
	pr := &Proposer{}
	pr.highestAccval = nil
	pr.highestAccseq = -1
	pr.highestPrenum = -1
	pr.resProposal = make([]Proposal, len(px.peers))
	pr.px = px
	pr.v = v
	pr.majority = len(px.peers)/2 + 1
	pr.reject = make(chan int)
	pr.done = make(chan bool)
	pr.peerNum = len(px.peers)
	pr.decided = true
	return pr
}

const sendInterval = time.Millisecond * 100

func (pr *Proposer) sendValue(seq int, v interface{}) {
	log.Printf("iterface is %s\n", pr.px.highestAccval)
	pr.seq = seq
	for {
		for _, v := range pr.resProposal {
			// init to zero as a special indicator to show this
			// proposal has not been set
			v.Seq = -1
			v.V = nil
		}
		// choose n, unique and higher than any n seen so far
		if v := pr.px.Max(); v > pr.seq {
			pr.seq = v
		}
		// send prepare(n) to all servers including self
		pr.doneNum = 0
		pr.successNum = 0
		pr.isreject = false

		for index, _ := range pr.px.peers {
			go pr.sendPrepare(index, pr.seq)
		}
		// wait for majority
		select {
		case replyNum := <-pr.reject:
			log.Printf("Justice is rejected\n")
			pr.seq = replyNum + 1
			<-pr.done
			time.Sleep(time.Duration(pr.px.me) * sendInterval)
			continue
		case <-pr.done:
			pr.mu.Lock()
			log.Printf("Justice is done\n")
			if pr.successNum < pr.majority {
				log.Printf("But not from majority num: %d/%d",
					pr.successNum, pr.majority)
				pr.mu.Unlock()
				time.Sleep(time.Duration(pr.px.me) * sendInterval)
				continue
			}
			pr.successNum = 0
			pr.doneNum = 0
			pr.mu.Unlock()
		}

		/* find the response with highest seq */
		highestNum := -1
		sendProposal := Proposal{}
		pr.isreject = false
		for _, response := range pr.resProposal {
			if response.Seq > highestNum {
				if response.V != nil {
					log.Printf("a unnil occur")
				}
				sendProposal.V = response.V
				highestNum = response.Seq
				pr.decided = false
			}
		}
		log.Printf("Done with find maximum response\n")
		if highestNum == -1 || sendProposal.V == nil {
			sendProposal.V = v
			log.Printf("The value is set as decided\n")
			pr.decided = true
		}
		sendProposal.Seq = pr.seq
		for index, _ := range pr.px.peers {
			go pr.sendAccept(index, sendProposal)
		}

		select {
		case <-pr.done:
			log.Printf("Accept justice is done\n")
			pr.mu.Lock()
			if pr.successNum < pr.majority {
				log.Printf("Accept But not from majority num: %d/%d",
					pr.successNum, pr.majority)
				time.Sleep(time.Duration(pr.px.me) * sendInterval)
				continue
			}
			pr.successNum = 0
			pr.doneNum = 0
			pr.mu.Unlock()
		case replyNum := <-pr.reject:
			log.Printf("Accept justice is rejected\n")
			pr.seq = replyNum + 1
			<-pr.done
			time.Sleep(time.Duration(pr.px.me) * sendInterval)
			continue
		}
		if pr.decided {
			log.Printf("The value is decided\n")
			for index, _ := range pr.px.peers {
				go pr.sendDecide(index, sendProposal)
			}
			<-pr.done
			pr.mu.Lock()
			pr.successNum = 0
			pr.doneNum = 0
			pr.mu.Unlock()
			return
		}
		log.Printf("End send\n")
	}
}

type DecideArgs struct {
	Proposal Proposal
}
type DecideReply struct {
}

func (pr *Proposer) sendDecide(index int, proposal Proposal) {
	args := &DecideArgs{proposal}
	var reply DecideReply
	call(pr.px.peers[index], "Paxos.RecDecide", args, &reply)
	pr.mu.Lock()
	pr.doneNum++
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}
func (px *Paxos) RecDecide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	px.agreeIns[args.Proposal.Seq] = args.Proposal.V
	px.highestAccval = nil
	px.mu.Unlock()
	return nil
}

type AcceptArgs struct {
	Proposal Proposal
}
type AcceptReply struct {
	Accept        bool
	HighestPrenum int
}

func (pr *Proposer) sendAccept(index int, proposal Proposal) {
	args := &AcceptArgs{proposal}
	var reply AcceptReply
	reply.Accept = true
	responded := call(pr.px.peers[index], "Paxos.RecAccept", args, &reply)
	pr.mu.Lock()
	if responded && reply.Accept {
		pr.successNum++
	} else if !reply.Accept {
		pr.mu.Lock()
		if !pr.isreject {
			pr.reject <- reply.HighestPrenum
			pr.isreject = true
		}
		pr.mu.Lock()
	}
	pr.doneNum++
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}

func (px *Paxos) RecAccept(args *AcceptArgs, reply *AcceptReply) error {
	log.Printf("RecAccept Paxos %s args.seq %d accept with highest seen num: %d\n",
		px.peers[px.me], args.Proposal.Seq, px.highestPre)
	reply.Accept = true
	if args.Proposal.Seq < px.highestPre {
		reply.Accept = false
		reply.HighestPrenum = px.highestPre
		return nil
	} else {
		reply.Accept = true
		px.highestAccseq = args.Proposal.Seq
		px.highestPre = args.Proposal.Seq
		px.highestAccval = args.Proposal.V
		return nil
	}
}

type PrepareArgs struct {
	Seq int
}

type PrepareReply struct {
	Accept        bool
	Proposal      Proposal
	HighestPrenum int
}

func (pr *Proposer) sendPrepare(index int, seq int) {
	args := &PrepareArgs{seq}
	var reply PrepareReply

	responded := call(pr.px.peers[index], "Paxos.RecPrepare", args, &reply)
	/* Almost always responded successfuly */
	if responded && reply.Accept {
		pr.mu.Lock()
		pr.successNum++
		pr.mu.Unlock()
	} else if !reply.Accept {
		pr.mu.Lock()
		if !pr.isreject {
			pr.reject <- reply.HighestPrenum
			pr.isreject = true
		}
		pr.mu.Unlock()
	}
	pr.mu.Lock()
	pr.resProposal[index] = reply.Proposal
	pr.doneNum++
	log.Printf("me is %s doneNum is %d\n", pr.px.peers[pr.px.me], pr.doneNum)
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}

/* RPC must start with capital letter */
func (px *Paxos) RecPrepare(args *PrepareArgs, reply *PrepareReply) error {
	if args.Seq > px.highestPre {
		log.Printf("Paxos %s args.seq %d accept with highest seen num: %d\n",
			px.peers[px.me], args.Seq, px.highestPre)
		reply.Accept = true
		reply.Proposal.Seq = px.highestAccseq
		reply.Proposal.V = px.highestAccval
		px.highestPre = args.Seq
	} else {
		log.Printf("Paxos %s rejected args.seq %d with highest seen num: %d\n",
			px.peers[px.me], args.Seq, px.highestPre)
		reply.Accept = false
		reply.HighestPrenum = px.highestPre
	}
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	var v int
	px.mu.Lock()
	if px.highestAccseq > px.highestPre {
		v = px.highestAccseq
	} else {
		v = px.highestPre
	}
	px.mu.Unlock()
	return v
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	if v, ok := px.agreeIns[seq]; ok {
		return ok, v
	} else {
		return ok, nil
	}
	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.highestAccseq = -1
	px.highestPre = -1
	px.agreeIns = make(map[int]interface{})
	px.proposer = make(map[int]Proposer)
	px.highestAccval = nil

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
