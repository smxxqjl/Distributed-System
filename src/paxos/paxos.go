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
	minNums    []int
	me         int // index into peers[]

	// Your data here.
	agreeIns  map[int]interface{}
	proposers map[int]*Proposer
	max       int
	sendMu    sync.Mutex
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

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()
	if _, ok := px.agreeIns[seq]; ok {
		px.mu.Unlock()
		return
	}
	if _, ok := px.proposers[seq]; !ok {
		prt := px.MakeProposer(seq)
		px.proposers[seq] = prt
	}
	pr := px.proposers[seq]
	px.mu.Unlock()
	go pr.sendValue(v)
}

type Proposal struct {
	Seq int
	V   interface{}
}

type Proposer struct {
	seq           int
	proNum        int
	resProposal   []Proposal
	px            *Paxos
	v             interface{}
	mu            sync.Mutex
	highestAccseq int
	highestPrenum int
	highestAccval interface{}
	majority      int
	// we share the same channel value for accept and prepare
	doneNum     int
	successNum  int
	done        chan bool
	reject      chan int
	decided     bool
	decidedchan chan bool
	peerNum     int
	isreject    bool
	accSet      bool
}

func (px *Paxos) MakeProposer(seq int) *Proposer {
	pr := &Proposer{}
	pr.decidedchan = make(chan bool)
	pr.seq = seq
	pr.highestAccval = nil
	pr.highestAccseq = -1
	pr.highestPrenum = -1
	pr.successNum = 0
	pr.resProposal = make([]Proposal, len(px.peers))
	pr.px = px
	pr.v = nil
	pr.majority = len(px.peers)/2 + 1
	pr.reject = make(chan int)
	pr.done = make(chan bool)
	pr.peerNum = len(px.peers)
	pr.decided = true
	pr.accSet = false
	return pr
}

const sendInterval = time.Millisecond * 100

func (pr *Proposer) sendValue(sV interface{}) {
	pr.px.sendMu.Lock()
	defer pr.px.sendMu.Unlock()
	pr.v = sV
	var minNum int
	for {
		pr.mu.Lock()
		// choose n, unique and higher than any n seen so far
		var xx int
		if pr.highestAccseq > pr.highestPrenum {
			xx = pr.highestAccseq + 1
		} else {
			xx = pr.highestPrenum + 1
		}
		if pr.proNum < xx {
			pr.proNum = xx
		}
		// send prepare(n) to all servers including self
		pr.doneNum = 0
		pr.successNum = 0
		pr.isreject = false
		pr.mu.Unlock()
		pr.px.mu.Lock()
		minNum = pr.px.minNums[pr.px.me]
		pr.px.mu.Unlock()

		for i, _ := range pr.resProposal {
			// init to zero as a special indicator to show this
			// proposal has not been set
			pr.resProposal[i].Seq = -1
			pr.resProposal[i].V = nil
		}
		//pr.printResponse()
		for index, _ := range pr.px.peers {
			go pr.sendPrepare(index, minNum, pr.px.me)
		}
		// wait for majority
		if pr.px.dead {
			return
		}
		select {
		case <-pr.decidedchan:
			return
		case replyNum := <-pr.reject:
			//pr.mu.Lock()
			pr.proNum = replyNum + 1
			//pr.mu.Unlock()
			<-pr.done
			time.Sleep(time.Duration(pr.px.me) * sendInterval)
			continue
		case <-pr.done:
			pr.mu.Lock()
			if pr.successNum < pr.majority {
				pr.mu.Unlock()
				//return
				//time.Sleep(time.Duration(pr.px.me) * sendInterval)
				continue
			}
			pr.successNum = 0
			pr.doneNum = 0
			pr.mu.Unlock()
		}

		//pr.printResponse()
		/* find the response with highest seq */
		highestNum := -1
		sendProposal := Proposal{}
		pr.isreject = false
		for _, response := range pr.resProposal {
			if response.Seq > highestNum {
				sendProposal.V = response.V
				highestNum = response.Seq
				pr.decided = false
			}
		}
		if highestNum == -1 || sendProposal.V == nil {
			sendProposal.V = sV
			pr.decided = true
		}
		pr.px.mu.Lock()
		minNum = pr.px.minNums[pr.px.me]
		pr.px.mu.Unlock()

		sendProposal.Seq = pr.proNum
		for index, _ := range pr.px.peers {
			go pr.sendAccept(index, sendProposal, minNum, pr.px.me)
		}

		if pr.px.dead {
			return
		}
		select {
		case <-pr.decidedchan:
			return
		case <-pr.done:
			pr.mu.Lock()
			if pr.successNum < pr.majority {
				time.Sleep(time.Duration(pr.px.me) * sendInterval)
				pr.mu.Unlock()
				continue
			}
			pr.successNum = 0
			pr.doneNum = 0
			pr.mu.Unlock()
		case replyNum := <-pr.reject:
			pr.proNum = replyNum + 1
			<-pr.done
			time.Sleep(time.Duration(pr.px.me) * sendInterval)
			continue
		}
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
}

type DecideArgs struct {
	Proposal Proposal
	Seq      int
}
type DecideReply struct {
}

func (pr *Proposer) sendDecide(index int, proposal Proposal) {
	args := &DecideArgs{proposal, pr.seq}
	var reply DecideReply
	if index == pr.px.me {
		pr.RecDecide(args, &reply)
		//pr.px.mu.Lock()
		if pr.seq > pr.px.max {
			pr.px.max = pr.seq
		}
		//pr.px.mu.Unlock()
	} else {
		pr.px.mu.Lock()
		name := pr.px.peers[index]
		pr.px.mu.Unlock()
		/* We don't handle the error if the callee did not reponded */
		call(name, "Paxos.PaxosRecDecide", args, &reply)
	}
	pr.mu.Lock()
	pr.doneNum++
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}

func (px *Paxos) PaxosRecDecide(args *DecideArgs, reply *DecideReply) error {
	var pr *Proposer
	var ok bool
	px.mu.Lock()
	if pr, ok = px.proposers[args.Seq]; !ok {
		px.proposers[args.Seq] = px.MakeProposer(args.Seq)
		pr = px.proposers[args.Seq]
	}
	if args.Seq > px.max {
		px.max = args.Seq
	}
	px.mu.Unlock()
	pr.RecDecide(args, reply)
	return nil
}

func (pr *Proposer) RecDecide(args *DecideArgs, reply *DecideReply) error {
	pr.px.mu.Lock()
	pr.px.agreeIns[pr.seq] = args.Proposal.V
	pr.px.mu.Unlock()
	//case pr.decidedchan <- true
	return nil
}

type AcceptArgs struct {
	Proposal Proposal
	Seq      int
	MinNum   int
	Caller   int
}
type AcceptReply struct {
	Accept        bool
	HighestPrenum int
}

func (pr *Proposer) sendAccept(index int, proposal Proposal, minNum int, caller int) {
	args := &AcceptArgs{proposal, pr.seq, minNum, caller}
	var reply AcceptReply
	reply.Accept = true
	responded := true
	if index == pr.px.me {
		pr.RecAccept(args, &reply)
	} else {
		pr.px.mu.Lock()
		name := pr.px.peers[index]
		pr.px.mu.Unlock()
		responded = call(name, "Paxos.PaxosRecAccept", args, &reply)
	}
	if responded && reply.Accept {
		pr.successNum++
	} else if !reply.Accept {
		pr.mu.Lock()
		if !pr.isreject {
			pr.reject <- reply.HighestPrenum
			pr.isreject = true
		}
		pr.mu.Unlock()
	}
	pr.mu.Lock()
	pr.doneNum++
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}

func (px *Paxos) PaxosRecAccept(args *AcceptArgs, reply *AcceptReply) error {
	var pr *Proposer
	var ok bool
	px.mu.Lock()
	px.minNums[args.Caller] = args.MinNum
	if pr, ok = px.proposers[args.Seq]; !ok {
		px.proposers[args.Seq] = px.MakeProposer(args.Seq)
		pr = px.proposers[args.Seq]
	}
	px.mu.Unlock()
	pr.RecAccept(args, reply)
	return nil
}

func (pr *Proposer) RecAccept(args *AcceptArgs, reply *AcceptReply) error {
	reply.Accept = true
	if args.Proposal.Seq < pr.highestPrenum {
		reply.Accept = false
		reply.HighestPrenum = pr.highestPrenum
		return nil
	} else {
		reply.Accept = true
		pr.mu.Lock()
		pr.highestAccseq = args.Proposal.Seq
		pr.highestPrenum = args.Proposal.Seq
		pr.highestAccval = args.Proposal.V
		pr.accSet = true
		pr.mu.Unlock()
		return nil
	}
}

type PrepareArgs struct {
	ProNum int
	Seq    int
	MinNum int
	Caller int
}

type PrepareReply struct {
	Accept        bool
	Proposal      Proposal
	HighestPrenum int
}

func (pr *Proposer) sendPrepare(index int, minNum int, caller int) {
	args := &PrepareArgs{pr.proNum, pr.seq, minNum, caller}
	var reply PrepareReply

	pr.px.mu.Lock()
	name := pr.px.peers[index]
	pr.px.mu.Unlock()
	responded := call(name, "Paxos.PaxosRecPrepare", args, &reply)
	/* Almost always responded successfuly */
	if responded && reply.Accept {
		pr.mu.Lock()
		pr.successNum++
		pr.mu.Unlock()
	} else if !reply.Accept && responded {
		pr.mu.Lock()
		if !pr.isreject {
			pr.reject <- reply.HighestPrenum
			pr.isreject = true
		}
		pr.mu.Unlock()
	}
	pr.mu.Lock()
	if reply.Accept && responded {
		pr.resProposal[index] = reply.Proposal
	}
	pr.doneNum++
	if pr.doneNum == pr.peerNum {
		pr.done <- true
	}
	pr.mu.Unlock()
}

func (px *Paxos) PaxosRecPrepare(args *PrepareArgs, reply *PrepareReply) error {
	var pr *Proposer
	var ok bool
	px.mu.Lock()
	px.minNums[args.Caller] = args.MinNum
	if pr, ok = px.proposers[args.Seq]; !ok {
		px.proposers[args.Seq] = px.MakeProposer(args.Seq)
		pr = px.proposers[args.Seq]
	}
	px.mu.Unlock()
	pr.RecPrepare(args, reply)
	return nil
}

/* RPC must start with capital letter */
func (pr *Proposer) RecPrepare(args *PrepareArgs, reply *PrepareReply) error {
	if args.ProNum > pr.highestPrenum {
		reply.Accept = true
		if pr.accSet {
			reply.Proposal.Seq = pr.highestAccseq
		} else {
			reply.Proposal.Seq = -1
		}
		reply.Proposal.V = pr.highestAccval
		pr.mu.Lock()
		pr.highestPrenum = args.ProNum
		pr.mu.Unlock()
	} else {
		reply.Accept = false
		reply.HighestPrenum = pr.highestPrenum
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
	px.mu.Lock()
	i := px.minNums[px.me]
	px.minNums[px.me] = seq
	for ; i < seq; i++ {
		delete(px.proposers, i)
		delete(px.agreeIns, i)
	}
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.max
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
	px.mu.Lock()
	minNum := px.minNums[0]
	for _, v := range px.minNums {
		if v < minNum {
			minNum = v
		}
	}
	px.mu.Unlock()
	return minNum + 1
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
	px.mu.Lock()
	defer px.mu.Unlock()
	if v, ok := px.agreeIns[seq]; ok {
		return ok, v
	} else {
		return ok, nil
	}
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
	px.max = -1

	// Your initialization code here.
	px.agreeIns = make(map[int]interface{})
	px.proposers = make(map[int]*Proposer)
	px.minNums = make([]int, len(px.peers))
	for i := 0; i < len(px.peers); i++ {
		px.minNums[i] = -1
	}

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
