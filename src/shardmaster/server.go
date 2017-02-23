package shardmaster

import "kvpaxos"
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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs  []Config // indexed by config num
	exeSeq   int
	queryMap map[int64]int
}

type Op struct {
	// Your data here.
	Operation  string
	Gid        int64
	Servers    []string
	Shard      int
	Identifier int64
	QueryIndex int
}

func (sm *ShardMaster) waitAgreement(seq int, operation Op) (bool, Op) {
	to := 10 * time.Millisecond
	for {
		if decided, v := sm.px.Status(seq); decided {
			reOp := v.(Op)
			return operation.Identifier == reOp.Identifier, reOp
		}
		time.Sleep(to)
		if to < 2*time.Second {
			to *= 2
		}
		if to >= 1*time.Second {
			operationActive := Op{"NULL", 0, nil, 0, kvpaxos.Nrand(), 0}
			sm.px.Start(seq, operationActive)
		}
	}
}

func runJoin(cConfig *Config, bConfig *Config, args *JoinArgs) {
	log.Printf("Start RunJoin")
	if _, ok := bConfig.Groups[args.GID]; ok {
		log.Printf("Already in %d\n", args.GID)
		cConfig.Groups = bConfig.Groups
		cConfig.Shards = bConfig.Shards
		return
	}
	/* Same as make(map[int64])string */
	cConfig.Groups = map[int64][]string{}
	for k, v := range bConfig.Groups {
		cConfig.Groups[k] = v
		//log.Printf("Copy group %s\n", v)
	}
	cConfig.Groups[args.GID] = args.Servers

	for i := 0; i < NShards; i++ {
		cConfig.Shards[i] = bConfig.Shards[i]
		//log.Printf("Copy shard %s\n", i)
	}

	gidIndex := make(map[int64][]int)
	bgroupNum := 0
	zeroGroup := false
	for index, shardGid := range bConfig.Shards {
		if shardGid == 0 {
			zeroGroup = true
			log.Printf("Zero Group\n")
			break
		}
		if _, ok := gidIndex[shardGid]; ok {
			gidIndex[shardGid] = append(gidIndex[shardGid], index)
		} else {
			gidIndex[shardGid] = make([]int, 0)
			gidIndex[shardGid] = append(gidIndex[shardGid], index)
			bgroupNum++
		}
	}

	if zeroGroup {
		for i := 0; i < NShards; i++ {
			cConfig.Shards[i] = args.GID
		}
	} else {
		cgroupNum := bgroupNum + 1
		perShardnum := NShards / cgroupNum
		k := perShardnum
		for _, group := range gidIndex {
			for i := len(group) - perShardnum; i > 0 && k > 0; i-- {
				cConfig.Shards[group[i-1]] = args.GID
				k--
			}
		}
	}
}
func runLeave(cConfig *Config, bConfig *Config, args *LeaveArgs) {

	if _, ok := bConfig.Groups[args.GID]; !ok {
		log.Printf("Not in %d\n", args.GID)
		cConfig.Groups = bConfig.Groups
		cConfig.Shards = bConfig.Shards
		return
	}
	/* Same as make(map[int64])string */
	cConfig.Groups = map[int64][]string{}
	for k, v := range bConfig.Groups {
		cConfig.Groups[k] = v
	}
	delete(cConfig.Groups, args.GID)

	for i := 0; i < NShards; i++ {
		cConfig.Shards[i] = bConfig.Shards[i]
	}

	gidIndex := make(map[int64][]int)
	bgroupNum := 0
	for index, shardGid := range bConfig.Shards {
		if _, ok := gidIndex[shardGid]; ok {
			gidIndex[shardGid] = append(gidIndex[shardGid], index)
		} else {
			gidIndex[shardGid] = make([]int, 0)
			gidIndex[shardGid] = append(gidIndex[shardGid], index)
			bgroupNum++
		}
	}

	cgroupNum := bgroupNum - 1
	gCount := 0
	for k := range cConfig.Groups {
		gCount++
		log.Printf("%s\n", k)
	}
	if cgroupNum == 0 {
		for i := 0; i < NShards; i++ {
			cConfig.Shards[i] = 0
		}
	} else if cgroupNum < 0 {
		panic("You can not leave!!\n")
	} else if gCount >= NShards {
		for k, _ := range cConfig.Groups {
			if _, ok := gidIndex[k]; !ok {
				if _, ok := gidIndex[args.GID]; ok {
					cConfig.Shards[gidIndex[args.GID][0]] = k
				}
			}
		}
	} else {
		perShardnum := NShards / cgroupNum
		bias := 0
		if cgroupNum != 0 || perShardnum == NShards/(cgroupNum-1) {
			bias = 1
		}
		k := len(gidIndex[args.GID])
		log.Printf("The number of shards to leave is %d and cgroupNum is %d\n", k, cgroupNum)
		for id, group := range gidIndex {
			log.Printf("perShardnum:%d and GroupLength:%d\n", perShardnum, len(group))
			if id == args.GID {
				continue
			}
			for i := perShardnum - len(group) + bias; i > 0 && k > 0; i-- {
				cConfig.Shards[gidIndex[args.GID][k-1]] = id
				log.Printf("Set Index %d as id %d\n", gidIndex[args.GID][k-1], id)
				k--
			}
		}
	}
}

func runMove(cConfig *Config, bConfig *Config, args *MoveArgs) {
	cConfig.Groups = bConfig.Groups
	for i := 0; i < NShards; i++ {
		cConfig.Shards[i] = bConfig.Shards[i]
	}
	cConfig.Shards[args.Shard] = args.GID
}

func (sm *ShardMaster) runQuery(num int, id int64) {
	highestConfig := len(sm.configs) - 1
	log.Printf("The higest is %d query is %d\n", highestConfig, num)
	if num <= -1 || num >= highestConfig {
		log.Printf("id:%d set as %d", id, highestConfig)
		sm.queryMap[id] = highestConfig
	} else {
		sm.queryMap[id] = num
	}
}

func (sm *ShardMaster) executeUntil(seq int) {
	var nullop Op
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for ; sm.exeSeq <= seq; sm.exeSeq++ {
		_, exeOp := sm.waitAgreement(sm.exeSeq, nullop)
		log.Printf("Seq:%d Come out Op %s GID:%d\n", sm.exeSeq, exeOp.Operation, exeOp.Gid)
		if exeOp.Operation == "Query" {
			sm.runQuery(exeOp.QueryIndex, exeOp.Identifier)
			continue
		}
		sm.configs = append(sm.configs, Config{})
		cConfig := &sm.configs[len(sm.configs)-1]
		bConfig := &sm.configs[len(sm.configs)-2]

		cConfig.Num = bConfig.Num + 1
		if exeOp.Operation == "Join" {
			var args JoinArgs
			args.GID = exeOp.Gid
			args.Servers = exeOp.Servers
			runJoin(cConfig, bConfig, &args)
		} else if exeOp.Operation == "Leave" {
			var args LeaveArgs
			args.GID = exeOp.Gid
			runLeave(cConfig, bConfig, &args)
		} else if exeOp.Operation == "Move" {
			var args MoveArgs
			args.GID = exeOp.Gid
			args.Shard = exeOp.Shard
			runMove(cConfig, bConfig, &args)
		}
		printfShard(cConfig)
	}
}

func printfShard(config *Config) {
	for i := range config.Shards {
		log.Printf("Shard:%d Server:%d\n", i, config.Shards[i])
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	var seq int
	log.Printf("Call Join\n")
	sendOp := Op{"Join", args.GID, args.Servers,
		0, kvpaxos.Nrand(), 0}
	for {
		seq = sm.px.Max() + 1
		sm.px.Start(seq, sendOp)
		if agree, _ := sm.waitAgreement(seq, sendOp); agree {
			break
		} else {
			continue
		}
	}

	log.Printf("Join finish agreement\n")

	log.Printf("Join is seq:%d\n", seq)
	sm.executeUntil(seq)
	sm.px.Done(seq)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	var seq int
	log.Printf("Call Leave\n")
	sendOp := Op{"Leave", args.GID, nil,
		0, kvpaxos.Nrand(), 0}
	for {
		seq = sm.px.Max() + 1
		sm.px.Start(seq, sendOp)
		if agree, _ := sm.waitAgreement(seq, sendOp); agree {
			break
		} else {
			continue
		}
	}
	log.Printf("Leave finish agreement\n")

	sm.executeUntil(seq)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	var seq int
	sendOp := Op{"Move", args.GID, nil,
		args.Shard, kvpaxos.Nrand(), 0}
	for {
		seq = sm.px.Max() + 1
		sm.px.Start(seq, sendOp)
		if agree, _ := sm.waitAgreement(seq, sendOp); agree {
			break
		} else {
			continue
		}
	}
	sm.executeUntil(seq)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	var seq int
	log.Printf("Start Query\n")
	sendOp := Op{"Query", 0, nil,
		0, kvpaxos.Nrand(), args.Num}
	for {
		seq = sm.px.Max() + 1
		sm.px.Start(seq, sendOp)
		if agree, _ := sm.waitAgreement(seq, sendOp); agree {
			break
		} else {
			continue
		}
	}
	sm.executeUntil(seq)

	sm.mu.Lock()
	reply.Config = sm.configs[sm.queryMap[sendOp.Identifier]]
	log.Printf("return No.%d config\n", sm.queryMap[sendOp.Identifier])
	sm.mu.Unlock()

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)
	sm.exeSeq = 0
	sm.queryMap = make(map[int64]int)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
