package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	primary        string
	backup         string
	primaryPingout int
	backupPingout  int
	primaryCrash   bool
	backupCrash    bool
	ack            uint
	currentView    View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	/* The latter condition is for init condition */
	if args.Viewnum == 0 && vs.currentView.Viewnum != 0 {
		//log.Printf("Zero depth ping !!!!!!!!!!!!!!!!")
		if args.Me == vs.currentView.Primary {
			vs.primaryCrash = true
		} else if args.Me == vs.currentView.Backup {
			vs.backupCrash = true
		}
	}
	if args.Me == vs.currentView.Primary && args.Viewnum > vs.ack {
		vs.ack = args.Viewnum
	}
	if args.Me == vs.currentView.Primary {
		vs.mu.Lock()
		vs.primaryPingout = 0
		vs.mu.Unlock()
	} else if args.Me == vs.currentView.Backup {
		vs.mu.Lock()
		vs.backupPingout = 0
		vs.mu.Unlock()
	}
	/* In the case that the view has been acknowledged
	/* View can be updated
	/* since the vs.ack and vs.currentView.Viewnum are both
	/* zero in the beginning the init case shall fall into
	/* this branch too.*/
	if vs.ack == vs.currentView.Viewnum {
		updateView := false
		if vs.backupCrash {
			vs.currentView.Backup = ""
			vs.mu.Lock()
			vs.backupPingout = 0
			vs.mu.Unlock()
			updateView = true
		}
		if vs.primaryCrash {
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = ""
			vs.mu.Lock()
			vs.primaryPingout = 0
			vs.mu.Unlock()
			updateView = true
		}
		if vs.currentView.Primary == "" && !vs.primaryCrash {
			vs.currentView.Primary = args.Me
			updateView = true
		} else if vs.currentView.Backup == "" && args.Me != vs.currentView.Primary {
			vs.currentView.Backup = args.Me
			updateView = true
		}
		vs.backupCrash = false
		vs.primaryCrash = false
		if updateView {
			vs.currentView.Viewnum++
		}
	}
	reply.View = vs.currentView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	//log.Printf("currentView: %s %s\n", vs.currentView.Primary, vs.currentView.Backup)
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	vs.mu.Lock()
	if vs.currentView.Primary != "" {
		vs.primaryPingout++
	}
	if vs.currentView.Backup != "" {
		vs.backupPingout++
	}
	vs.mu.Unlock()

	if vs.primaryPingout >= DeadPings {
		//log.Printf("Set primaryCrash\n")
		vs.primaryCrash = true
	} else if vs.backupPingout >= DeadPings {
		//log.Printf("Set Backup Crash\n")
		vs.backupCrash = true
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.primary = ""
	vs.backup = ""
	vs.primaryPingout = 0
	vs.backupPingout = 0
	vs.primaryCrash = false
	vs.backupCrash = false
	vs.ack = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
