package kvpaxos

import "strconv"
import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"

import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.Debug

	// Two type of Operation "Get" "Put". Get with one argument
	// Key, Put and Puthash with the same operation string "Put" while puthash
	// set doHash true.
	Operation  string
	Key        string
	Value      string
	DoHash     bool
	Identifier int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.

	// The kvpaxos will maintain an opnumber for unique the op by making
	// the string me:opSeq
	opNum      int
	exeSeq     int
	keyValue   map[string]string
	executedOp map[int64]string
}

func (kv *KVPaxos) waitAgreement(seq int, operation Op) (bool, Op) {
	to := 10 * time.Millisecond
	for {
		if decided, v := kv.px.Status(seq); decided {
			reOp := v.(Op)
			log.Printf("Choosen id:%d and the require id is %d",
				reOp.Identifier, operation.Identifier)
			return operation.Identifier == reOp.Identifier, reOp
		}
		log.Printf("seq %d not decided wating...\n", seq)
		time.Sleep(to)
		if to < 2*time.Second {
			to *= 2
		}
		if to >= 1*time.Second {
			log.Printf("Active go on\n")
			operationActive := Op{"Get", "", "", false, nrand()}
			kv.px.Start(seq, operationActive)
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	sendOp := Op{"Get", args.Key, "", false,
		args.Identifier}
	var seq int
	for {
		seq = kv.px.Max() + 1
		log.Printf("Start agree with %d id:%d\n", seq, args.Identifier)
		kv.px.Start(seq, sendOp)
		if agree, _ := kv.waitAgreement(seq, sendOp); agree {
			break
		} else {
			log.Printf("not choosen in this round\n")
			continue
		}
	}

	/*
		to := 10 * time.Millisecond
		for kv.exeSeq < seq {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}

		}

		log.Printf("After waiting")
		kv.mu.Lock()
		if val, ok := kv.executedOp[args.Identifier]; ok {
			reply.Value = val
		} else {
			if vall, okk := kv.keyValue[args.Key]; okk {
				reply.Value = vall
			} else {
				reply.Value = ""
			}
			kv.executedOp[args.Identifier] = reply.Value
		}
		log.Printf("Execute done\n")
		kv.exeSeq++
		kv.mu.Unlock()
	*/
	log.Printf("SerNun:%d Start to execute %d/%d\n", kv.me, kv.exeSeq, seq)
	kv.executeUntil(seq)
	kv.mu.Lock()
	reply.Value = kv.executedOp[args.Identifier]
	kv.px.Done(seq)
	kv.mu.Unlock()

	return nil
}

// This function execute all the agreed instance before seq
func (kv *KVPaxos) executeUntil(seq int) {
	var nullop Op
	kv.mu.Lock()
	for ; kv.exeSeq <= seq; kv.exeSeq++ {
		_, exeOp := kv.waitAgreement(kv.exeSeq, nullop)
		if _, alreadyExecute := kv.executedOp[exeOp.Identifier]; alreadyExecute {
			continue
		} else {
			if exeOp.Operation == "Get" {
				if value, keyexist := kv.keyValue[exeOp.Key]; keyexist {
					kv.executedOp[exeOp.Identifier] = value
				} else {
					kv.executedOp[exeOp.Identifier] = ""
				}
			} else if exeOp.Operation == "Put" {
				if _, vExist := kv.keyValue[exeOp.Key]; !vExist {
					kv.keyValue[exeOp.Key] = ""
				}
				if exeOp.DoHash {
					kv.executedOp[exeOp.Identifier] = kv.keyValue[exeOp.Key]
					hashnum := hash(kv.keyValue[exeOp.Key] + exeOp.Value)
					kv.keyValue[exeOp.Key] = strconv.Itoa(int(hashnum))
				} else {
					kv.executedOp[exeOp.Identifier] = kv.keyValue[exeOp.Key]
					kv.keyValue[exeOp.Key] = exeOp.Value
				}
			} else {
				log.Printf("Unknown method %s can not execute\n", exeOp.Operation)
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	var seq int
	sendOp := Op{"Put", args.Key, args.Value,
		args.DoHash, args.Identifier}
	for {
		seq = kv.px.Max() + 1
		log.Printf("Start agree with %d id:%d\n", seq, args.Identifier)
		kv.px.Start(seq, sendOp)
		if agree, _ := kv.waitAgreement(seq, sendOp); agree {
			break
		} else {
			continue
		}
	}

	/*
		for kv.exeSeq < seq {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}

		}
			log.Printf("After waiting\n")
			kv.mu.Lock()
			if val, ok := kv.executedOp[args.Identifier]; ok {
				reply.PreviousValue = val
			} else {
				if args.DoHash {
					if _, ok := kv.keyValue[args.Key]; !ok {
						kv.keyValue[args.Key] = ""
					}
					reply.PreviousValue = kv.keyValue[args.Key]
					kv.executedOp[args.Identifier] = reply.PreviousValue
					hashnum := hash(kv.keyValue[args.Key] + args.Value)
					kv.keyValue[args.Key] = strconv.Itoa(int(hashnum))
				} else {
					kv.keyValue[args.Key] = args.Value
				}
			}
			log.Printf("Done execute")
			kv.exeSeq++

			kv.mu.Unlock()
	*/
	log.Printf("SerNun:%d Start to execute %d/%d\n", kv.me, kv.exeSeq, seq)
	kv.executeUntil(seq)
	kv.mu.Lock()
	reply.PreviousValue = kv.executedOp[args.Identifier]
	kv.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.keyValue = make(map[string]string)
	kv.executedOp = make(map[int64]string)
	kv.exeSeq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
