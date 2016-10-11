
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  primary string
  backup string
  primaryPingout int
  backupPingout int
  primaryCrash bool
  backupCrash bool
  ack bool
  currentView View
  sendView View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  /*
  if vs.primary == "" {
    vs.primary = args.Me
    vs.view.Viewnum++
  } else if vs.backup == "" {
    vs.backup = args.Me
  } else if vs.primary == args.Me {
    vs.mu.Lock()
    vs.primaryPingout = 0
    vs.mu.Unlock()
    if vs.primaryCrash {
      vs.primary = ""
      vs.primaryCrash = false
      vs.primary = vs.backup
      vs.view.Viewnum++
    }
    if vs.backupCrash {
      vs.backup = ""
      vs.backupCrash = false
      vs.view.Viewnum++
    }
    if (len(vs.view.Backup) == 0) && (len(vs.backup) >= 0) {
      reply.View.Backup = vs.backup
      vs.view.Viewnum++
    }
  } else if vs.backup == args.Me {
    vs.mu.Lock()
    vs.backupPingout = 0
    vs.mu.Unlock()
  }
  reply.View.Primary = vs.primary
  reply.View.Viewnum = vs.view.Viewnum
  vs.view = reply.View
  log.Printf("%s %s %s %s\n", args.Me, vs.primary, reply.View.Primary, vs.view.Primary)
  return nil
  */

  // New code here
  /*
  log.Printf(" ")
  log.Printf("A ping: primary:%s backup:%s ping:%s\n", vs.view.Primary, vs.view.Backup, args.Me)
  if vs.view.Primary == args.Me {
    log.Printf("Primary ping\n")
    vs.ack = true
    vs.mu.Lock()
    vs.primaryPingout = 0
    vs.mu.Unlock()
    if vs.primaryCrash {
      log.Printf("I am here\n")
      vs.ack = false
      if vs.view.Backup != "" {
	vs.view.Primary = vs.view.Backup
	vs.view.Backup = args.Me
      } else {
	vs.view.Primary = args.Me
      }
      vs.view.Viewnum++
      vs.primaryCrash = false
    } else if vs.backupCrash {
      vs.ack = false
      vs.view.Backup = ""
      vs.view.Viewnum++
      vs.backupCrash = false
    }
  } else if vs.view.Primary == "" {
    log.Printf("Set up primary\n")
    vs.ack = false
    vs.nowview.Primary = args.Me
    vs.nowview.Viewnum++
    vs.view = vs.nowview
  } else if vs.backup == args.Me {
    log.Printf("bakcup ping")
    vs.mu.Lock()
    vs.backupPingout = 0
    vs.mu.Unlock()
  } else if vs.backup {
    log.Printf("set new backup")
    vs.ack = false
    vs.nowview.Backup = args.Me
    vs.nowview.Viewnum++
  } 
  reply.View = vs.view
  return nil
  */
  updateView := false

  log.Printf("A ping: args:Me:%s primary:%s backup:%s\n", args.Me, vs.currentView.Primary, vs.currentView.Backup)
  if args.Viewnum == 0 {
    log.Printf("Zero depth ping !!!!!!!!!!!!!!!!")
    if args.Me == vs.currentView.Primary {
      vs.primaryCrash = true
    } else if args.Me == vs.currentView.Backup {
      vs.backupCrash = false
    }
  }

  if vs.backupCrash {
    log.Printf("Handle backupCrash\n")
    vs.currentView.Backup = ""
    updateView = true
    vs.backupCrash = false
    vs.mu.Lock()
    vs.backupPingout = 0
    vs.mu.Unlock()
  }
  if vs.primaryCrash {
    log.Printf("Handle primaryCrash\n")
    vs.currentView.Primary = vs.currentView.Backup
    vs.currentView.Backup = ""
    updateView = true
    vs.primaryCrash = false
    vs.mu.Lock()
    vs.primaryPingout = 0
    vs.mu.Unlock()
  }
  if args.Me == vs.currentView.Primary {
    vs.mu.Lock()
    vs.primaryPingout = 0
    vs.mu.Unlock()
  } else if args.Me == vs.currentView.Backup {
    vs.mu.Lock()
    vs.backupPingout = 0
    vs.mu.Unlock()
  } else if vs.currentView.Primary == "" {
    vs.currentView.Primary = args.Me
    updateView = true
  } else if vs.currentView.Backup == "" {
    vs.currentView.Backup = args.Me
    updateView = true
  }
  if updateView {
    //log.Printf("Add new num")
    vs.currentView.Viewnum++
  }

  if args.Me == vs.sendView.Primary || vs.sendView.Primary == "" {
    vs.sendView = vs.currentView
  }
  log.Printf("Send view: primary:%s backup:%s\n", vs.sendView.Primary, vs.sendView.Backup)
  reply.View = vs.sendView
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = vs.currentView
  log.Printf("currentView: %s %s\n", vs.currentView.Primary, vs.currentView.Backup)
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
    log.Printf("Set primaryCrash\n")
    vs.primaryCrash = true
  } else if vs.backupPingout >= DeadPings {
    log.Printf("Set Backup Crash\n")
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
  vs.ack = true

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
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
