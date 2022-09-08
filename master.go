/* ----------------------------------
*  @author suyame 2022-08-31 15:37:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package mapreduceDemo

import (
	"encoding/json"
	"fmt"
	"log"
	"mapreduceDemo/loadbalance"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// master状态
// MASTER_INIT 处于加载map阶段
// MAP_FINISHED
// REDUCE_FINISHED
const (
	MASTER_INIT = 1 << iota
	MAP_FINISHED
	REDUCE_FINISHED
)

// Master 系统协调者
type Master struct {
	sync.RWMutex
	nMap            int                  // 需要处理的map任务数量
	nReduce         int                  // 需要处理的reduce任务数量
	nMapFinished    int                  // 已经完成处理的map任务数量
	nReduceFinished int                  // 已经完成处理的map任务数量
	state           uint8                // master 的状态（初始化|map任务完成|reduce任务完成）
	mapTasks        map[int]*Task        // key为TaskID, 快速查到指定Task
	reduceTasks     map[int]*Task        // key为TaskID, 快速查到指定Task
	workers         []loadbalance.Server // 注册到系统的所有worker
	rr              *loadbalance.RR      // 负载均衡算法
}

// NewMaster 根据所有workers信息创建master
func NewMaster(workers []loadbalance.Server) *Master {
	rr, err := loadbalance.NewRR(workers)
	if err != nil {
		panic(err)
	}
	m := &Master{
		state:       MASTER_INIT,
		mapTasks:    make(map[int]*Task, 0),
		reduceTasks: make(map[int]*Task, 0),
		workers:     workers,
		rr:          rr,
	}
	// 启动rpc服务
	m.server()
	return m
}

// Call 使用rpc调用worker的start函数
func (m *Master) Call(worker *Worker, task *Task, k, v interface{}) error {
	conn, err := rpc.DialHTTP("tcp", "127.0.0.1"+worker.Port)
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}
	// 对task序列化
	taskseq, err := json.Marshal(task)
	if err != nil {
		log.Println("json err: ", err)
		return err
	}
	req := WorkerRequest{
		Action: "start to run",
		Key:    k,
		Value:  v,
		Task:   taskseq,
	}
	var res WorkerResponse

	err = conn.Call("Worker.Start", req, &res) // 远程调用
	if err != nil {
		log.Println("Worker.Start error: ", err)
		return err
	}
	if res.Value == "retry" {
		// 需要重试
		log.Println("重试~~~~")
		time.Sleep(800 * time.Millisecond)
		m.Retry(task, k, v, "Worker不够用")
	}
	fmt.Printf("向worker 发送运行task: %d 通知, 响应状态码： %d, 值： %v\n", task.Id, res.StateCode, res.Value)
	return nil
}

// 如果task在10s后还没完成，就重新分配
func (m *Master) Retry(task *Task, k, v interface{}, info string) error {
	if task.State == TASK_DONE {
		return nil
	}
	log.Println("Send Retry for task: ", task.Id, " .... Because: ", info)
	// 从workers中选取一个空闲worker让他处理
	// 采用简单的轮询
	hit, err := m.rr.Do()
	if err != nil {
		return err
	}
	worker := hit.(*Worker)
	// worker.Register(&task)
	// 通知woker可以开始执行
	err = m.Call(worker, task, k, v)
	time.AfterFunc(300*time.Millisecond, func() {
		m.Retry(task, k, v, "超时")
	})
	return err
}

// AddTask 将任务添加到系统中，稍后将会申请worker去执行它
func (m *Master) AddTask(task *Task, k, v interface{}) error {
	m.Lock()
	switch task.Type {
	case MAP_TYPE:
		m.nMap++
		m.mapTasks[task.Id] = task
	case REDUCE_TYPE:
		m.nReduce++
		m.reduceTasks[task.Id] = task
	default:
		m.Unlock()
		return TaskTypeErr
	}
	m.Unlock()
	// 从workers中选取一个空闲worker让他处理
	// 采用简单的轮询
	hit, err := m.rr.Do()
	if err != nil {
		return err
	}
	worker := hit.(*Worker)
	// 通知woker可以开始执行
	err = m.Call(worker, task, k, v)
	// 超过10s还没有完成将重新分配
	time.AfterFunc(10*time.Second, func() {
		if task.State != TASK_DONE {
			m.Retry(task, k, v, "超时10s还没完成将重新分配task给空闲worker")
		}
	})
	return err
}

// server 启动rpc服务
func (m *Master) server() {
	rpc.Register(m)
	rpc.Register(Task{})
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1111")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 当worker完成任务后调用
func (m *Master) DoneWork(req WorkerRequest, res *WorkerResponse) error {
	if req.Action != "done work" {
		res.StateCode = 401
		return WokerReqTypeErr
	}
	res.StateCode = 200
	// 如果task执行失败，会在请求中传入k，v
	k := req.Key
	v := req.Value
	taskseq := req.Task
	// 反序列化
	task := &Task{}
	err := json.Unmarshal(taskseq, task)
	if err != nil {
		log.Println("json umarshal err")
		return err
	}
	taskID, taskType, taskState := task.Id, task.Type, task.State

	fmt.Printf("master 收到task: %d 完成通知, 请求类型 %v, 请求值 %v\n", taskID, req.Action, req.Value)
	if taskState != TASK_DONE {
		// 重试
		m.RLock()
		defer m.RUnlock()
		var task *Task
		var ok bool
		switch taskType {
		case MAP_TYPE:
			// 找到task
			task, ok = m.mapTasks[taskID]
		case REDUCE_TYPE:
			m.nReduce--
			// 找到task
			task, ok = m.reduceTasks[taskID]
		default:
			return TaskTypeErr
		}
		if !ok {
			return TaskNotFoundErr
		}
		return m.Retry(task, k, v, "任务运行失败！")
	}
	return m.DoneTask(taskID, taskType)
}

// DoneTask 当worker完成任务时调用
func (m *Master) DoneTask(taskID int, taskType uint8) error {
	m.Lock()
	defer m.Unlock()
	switch taskType {
	case MAP_TYPE:
		m.nMapFinished++
		// 找到task
		task, ok := m.mapTasks[taskID]
		if !ok {
			return TaskNotFoundErr
		}
		task.State = TASK_DONE
	case REDUCE_TYPE:
		m.nReduceFinished++
		// 找到task
		task, ok := m.reduceTasks[taskID]
		if !ok {
			return TaskNotFoundErr
		}
		task.State = TASK_DONE
	default:
		return TaskTypeErr
	}
	// 调整master状态
	if m.state == MASTER_INIT && m.nMap == m.nMapFinished {
		m.state = MAP_FINISHED
	} else if m.state == MAP_FINISHED && m.nReduce == m.nReduceFinished {
		m.state = REDUCE_FINISHED
	}
	return nil
}

// 等待master的map任务全部完成
func (m *Master) WaitMapTask() {
	for m.state != MAP_FINISHED {
		//log.Println("Waiting for map tasks done...")
		log.Println("MapFishiedNum: ", m.nMapFinished)
		time.Sleep(100 * time.Millisecond)
	}
	log.Println("All Map Task Finished!")
}

// 等待master的reduce任务全部完成
func (m *Master) WaitReduceTask() {
	for m.state != REDUCE_FINISHED {
		//log.Println("Waiting for map tasks done...")
		log.Println("ReduceFishiedNum: ", m.nReduceFinished)
		time.Sleep(100 * time.Millisecond)
	}
	log.Println("All Reduce Task Finished!")
}
