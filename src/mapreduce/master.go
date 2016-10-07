package mapreduce
import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

// Function define by myself which updates the info
// of mr concurrently
func (mr *MapReduce) UpdateInfo() {
  //var needWorker bool
  for {
    select {
    case newWorker := <- mr.registerChannel:
      log.Printf("%s is registering\n", newWorker)
      mr.Workers[newWorker] = &WorkerInfo{newWorker}
      mr.AvailWorkers[newWorker] = mr.Workers[newWorker]

    case <- mr.RequestWoker:
      mr.SendWorker()
      /*
      fmt.Printf("we have a request\n")
      var workerName string
      for _,v := range mr.AvailWorkers {
	mr.AvailWorkerChannel <- v.address
	log.Printf("Update to channel the %s is available again\n", v.address)
	workerName = v.address
	break
      }
      */
      //delete(mr.AvailWorkers, workerName)
    case freeWorker := <- mr.DoneWork:
      log.Printf("A done signal receive\n")
      mr.ReamainMaps--
      mr.RemainJobs--
      mr.AvailWorkers[freeWorker] = mr.Workers[freeWorker]
      if mr.ReamainMaps == 0 {
	mr.DoneMaps <- true
      }
      if mr.RemainJobs == 0 {
	mr.DoneJob <- true
	return
      }
    default:
      //if needWorker && len(mr.AvailWorkers) != 0 {
      mr.SendWorker()
      //}
    }
  }
}

func (mr *MapReduce) SendWorker() {
  var workerName string
  for _,v := range mr.AvailWorkers {
    mr.AvailWorkerChannel <- v.address
    log.Printf("Update to channel the %s is available again\n", v.address)
    workerName = v.address
    break
  }
  for _,v := range mr.AvailWorkers {
    log.Printf("we have %s\n", v)
  }
  delete(mr.AvailWorkers, workerName)
}
func (mr *MapReduce) failHandler(args interface{}) {
  nowWorker := <- mr.AvailWorkerChannel
  var reply DoJobReply
  ok := call(nowWorker, "Worker.DoJob", args, &reply)
  if ok == false {
    go mr.failHandler(args)
  } else {
    log.Printf("Fix the fail\n")
    mr.DoneWork <- nowWorker
  }
}
func (mr *MapReduce)SendJob(desWorker string, callOp JobType, jobNum int, NumOtherPhase int) {
  args := &DoJobArgs{}
  args.File = mr.file
  args.Operation = callOp
  args.JobNumber = jobNum 
  args.NumOtherPhase = NumOtherPhase

  log.Printf("SendSignalNow\n")
  var reply DoJobReply
  ok := call(desWorker, "Worker.DoJob", args, &reply)
  if ok == false {
    fmt.Printf("xxxxx Call: RPC %s %s error\n", desWorker, callOp)
    go mr.failHandler(args)
  } else {
    mr.DoneWork <- desWorker
  }
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  go mr.UpdateInfo()
  log.Printf("Start the master\n")
  mr.RequestWoker <- true
  for i := 0; i < mr.nMap; i++ {
    log.Printf("Waiting a worker\n")
    nowWorker := <- mr.AvailWorkerChannel
    log.Printf("A worker comes\n")
    go mr.SendJob(nowWorker, Map, i, mr.nReduce)
    mr.RequestWoker <- true
  }
  // Wait until maps are done
  <- mr.DoneMaps;
  log.Printf("----------Map is finished\n")
  mr.RequestWoker <- true
  for i := 0; i < mr.nReduce; i++ {
    log.Printf("Reduce %d Waiting a worker\n", i)
    nowWorker := <- mr.AvailWorkerChannel
    go mr.SendJob(nowWorker, Reduce, i, mr.nMap)
    log.Printf("A worker comes\n")
    mr.RequestWoker <- true
  }
  log.Printf("----------Reduce is finished\n")
  <- mr.DoneJob
  return mr.KillWorkers()
}
