/* ----------------------------------
*  @author suyame 2022-08-31 15:48:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package mapreduceDemo

import (
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
)

const (
	MAP_TYPE = iota
	REDUCE_TYPE
	TASK_INIT
	TASK_RUNNING
	TASK_DONE
)

type Task struct {
	Id       int    `json:"id"`
	SaveFile string `json:"save_file"`
	State    uint8  `json:"state"`
	Type     uint8  `json:"type"`
	// Plugin   func(k, v interface{}) `json:"plugin"`
}

var id int32

func NewMapTask(filename string) Task {
	return Task{
		Id:       int(atomic.AddInt32(&id, 1)),
		SaveFile: filename,
		State:    TASK_INIT,
		Type:     MAP_TYPE,
		// Plugin:   f,
	}
}
func NewReduceTask(filename string) Task {
	return Task{
		Id:       int(atomic.AddInt32(&id, 1)),
		SaveFile: filename,
		State:    TASK_INIT,
		Type:     REDUCE_TYPE,
	}
}

func (t *Task) Run(k, v interface{}) error {
	t.State = TASK_RUNNING
	switch t.Type {
	case MAP_TYPE:
		// mapfunc := t.Plugin.(func(k, v interface{}) []KV)
		mapfunc := Map
		v = mapfunc(k, v)
	case REDUCE_TYPE:
		// reducefunc := t.Plugin.(func(k, v interface{}) string)
		reducefunc := Reduce
		v = reducefunc(k, v)
		fmt.Println(v)
	default:
		return TaskTypeErr
	}
	// 将v写到文件中
	file, err := os.Create(t.SaveFile)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.Encode(v)
	file.Close()
	// 运行完成
	t.State = TASK_DONE
	return nil
}
