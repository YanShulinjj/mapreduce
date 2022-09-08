/* ----------------------------------
*  @author suyame 2022-08-31 17:02:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/
package mapreduceDemo

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const (
	WORKER_READY = iota
	WORKER_RUNNING
	WORKER_CRASHED
)

type Worker struct {
	task  *Task
	State uint8
	Port  string
}

type WorkerRequest struct {
	Action string
	Key    interface{}
	Value  interface{}
	Task   []byte
}
type WorkerResponse struct {
	StateCode int
	Value     interface{}
}

func NewWorker(port string) *Worker {
	w := &Worker{
		Port: port,
	}
	w.server()
	return w
}

func (w *Worker) GetState(req WorkerRequest, res *WorkerResponse) error {
	if req.Action != "get state" {
		return WokerReqTypeErr
	}
	res.StateCode = 200
	res.Value = w.State
	return nil
}

func (w *Worker) Start(req WorkerRequest, res *WorkerResponse) error {
	log.Println(w.Port, "开始处理...")
	if req.Action != "start to run" {
		res.StateCode = 404
		return WokerReqTypeErr
	}
	if w.task != nil && w.task.State != TASK_DONE {
		res.StateCode = 200
		res.Value = "retry"
		return nil
	}
	//w.State = WORKER_RUNNING
	res.StateCode = 200
	res.Value = "ok"
	taskseq := req.Task
	// 反序列化
	task := &Task{}
	// w.Register(&task)
	log.Println(string(taskseq))
	err := json.Unmarshal(taskseq, task)
	w.Register(task)
	if err != nil {
		return err
	}
	go func() {
		err := w.Do(req.Key, req.Value)
		if err != nil {
			panic(err)
		}
	}()
	log.Printf("worker 收到task开始运行请求, 请求类型 %v, 请求值 %v\n", req.Action, req.Value)
	return nil
}

func (w *Worker) server() {
	rpc.Register(w)
	// rpc.Register(Task{})
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", w.Port)
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (w *Worker) Register(task *Task) error {
	if w.task != nil && w.task.State != TASK_DONE {
		return TaskCrashErr
	}
	w.task = task
	return nil
}

// 执行Task
func (w *Worker) Do(k, v interface{}) error {
	if w.task == nil {
		return TaskNotResigterErr
	}
	log.Println(w.Port, "Start Runing Task...")
	err := w.task.Run(k, v)
	log.Println(w.Port, "Task: ", w.task.Id, " finished")
	err = w.Done(k, v)
	return err

}

// 执行完成后通知master
func (w *Worker) Done(k, v interface{}) error {
	conn, err := rpc.DialHTTP("tcp", "127.0.0.1:1111")
	if err != nil {
		log.Fatalln("dailing error: ", err)
		return err
	}

	taskseq, err := json.Marshal(w.task)
	req := WorkerRequest{
		Action: "done work",
		Task:   taskseq,
		Key:    k,
		Value:  v,
	}
	var res WorkerResponse
	log.Println("worker 向master 发起task完成请求")
	err = conn.Call("Master.DoneWork", req, &res) // 乘
	if err != nil {
		log.Fatalln("Master error: ", err)
		return err
	}
	log.Printf("worker 发起task完成通知, 响应码：%d, 值: %v\n", res.StateCode, res.Value)
	//w.State = WORKER_READY
	return nil
}

func (w *Worker) IsAlive() bool {
	return w.State == WORKER_READY
}
