package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk // no need to edit
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.

	currentView viewservice.View
	vshost      string
	keyvalue    map[string]string
	handleReq   map[int64]string
	mu          sync.Mutex
	reqMu       sync.Mutex
}

/* This function is called from RPC when primary want to send new put
/* to its backup, for simpility the same args is used as Put()
*/
func (pb *PBServer) Putbackup(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	pb.keyvalue[args.Key] = args.Value
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) InitBackup(args *InitBackupArgs, reply *InitBackupReply) error {
	pb.mu.Lock()
	for k, v := range *args.Keyvalue {
		pb.keyvalue[k] = v
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	if pb.currentView.Primary != pb.me {
		reply.Err = "NotMe"
		return nil
	}
	if val, ok := pb.handleReq[args.Reqnum]; ok {
		pb.reqMu.Lock()
		reply.PreviousValue = val
		pb.reqMu.Unlock()
		return nil
	}
	pb.mu.Lock()
	if args.DoHash {
		_, ok := pb.keyvalue[args.Key]
		if !ok {
			pb.keyvalue[args.Key] = ""
		}
		reply.PreviousValue = pb.keyvalue[args.Key]
		pb.handleReq[args.Reqnum] = pb.keyvalue[args.Key]
		hashnum := hash(pb.keyvalue[args.Key] + args.Value)
		pb.keyvalue[args.Key] = strconv.Itoa(int(hashnum))
	} else {
		pb.keyvalue[args.Key] = args.Value
	}
	value := pb.keyvalue[args.Key]
	pb.mu.Unlock()
	if pb.currentView.Backup != "" {
		argsBack := &PutbackupArgs{args.Key, value, args.Reqnum}
		var replyBack PutbackupReply
		call(pb.currentView.Backup, "PBServer.Putbackup", argsBack, &replyBack)
	}
	return nil
}

/*
func (pb *PBServer) ShowAll() {
	log.Printf("The Server:%s\n", pb.me)
	for k, v := range pb.keyvalue {
		log.Printf("Key:%s,Value:%s\n", k, v)
	}
}
*/

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	value, ok := pb.keyvalue[args.Key]
	pb.mu.Unlock()
	reply.Err = ""
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	args := &viewservice.PingArgs{pb.me, pb.currentView.Viewnum}
	var reply viewservice.PingReply

	call(pb.vshost, "ViewServer.Ping", args, &reply)
	if pb.me == pb.currentView.Primary &&
		pb.currentView.Backup != reply.View.Backup &&
		reply.View.Backup != "" {
		initArgs := &InitBackupArgs{}
		var initReply InitBackupReply
		initArgs.Keyvalue = &pb.keyvalue
		call(reply.View.Backup, "PBServer.InitBackup", initArgs, &initReply)
	}
	pb.currentView = reply.View
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.currentView.Primary = ""
	pb.currentView.Backup = ""
	pb.currentView.Viewnum = 0
	pb.vshost = vshost
	pb.keyvalue = make(map[string]string)
	pb.handleReq = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
