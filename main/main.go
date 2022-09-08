/* ----------------------------------
*  @author suyame 2022-08-31 20:26:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/
package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"mapreduceDemo"
	"mapreduceDemo/loadbalance"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

var (
	workers = []loadbalance.Server{
		&mapreduceDemo.Worker{
			Port: ":1201",
		},
		&mapreduceDemo.Worker{
			Port: ":1202",
		},
		&mapreduceDemo.Worker{
			Port: ":1203",
		},
		&mapreduceDemo.Worker{
			Port: ":1204",
		},
		&mapreduceDemo.Worker{
			Port: ":1205",
		},
		&mapreduceDemo.Worker{
			Port: ":1206",
		},
		&mapreduceDemo.Worker{
			Port: ":1207",
		},
		&mapreduceDemo.Worker{
			Port: ":1208",
		},
		&mapreduceDemo.Worker{
			Port: ":1209",
		},
		&mapreduceDemo.Worker{
			Port: ":1210",
		},
		&mapreduceDemo.Worker{
			Port: ":1211",
		},
		&mapreduceDemo.Worker{
			Port: ":1212",
		},
		&mapreduceDemo.Worker{
			Port: ":1213",
		},
		&mapreduceDemo.Worker{
			Port: ":1214",
		},
		&mapreduceDemo.Worker{
			Port: ":1215",
		},
		&mapreduceDemo.Worker{
			Port: ":1216",
		},
	}
)

type KVSort []mapreduceDemo.KV

func (k KVSort) Len() int {
	return len(k)
}
func (k KVSort) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
func (k KVSort) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

func main() {
	files, _ := ioutil.ReadDir("./data/")

	master := mapreduceDemo.NewMaster(workers)

	//start := time.Now()
	// 执行所有map任务
	for _, f := range files {
		log.Println("Parsing File: ", f.Name())
		// 新建一个MapTask
		task := mapreduceDemo.NewMapTask(filepath.Join("./mapdata/", "map_"+f.Name()))
		go master.AddTask(&task, "", f.Name())
		// intermediate = append(intermediate, kva...)
	}
	master.WaitMapTask()
	// 执行所有reduce任务

	files, _ = ioutil.ReadDir("./mapdata/")
	kva := []mapreduceDemo.KV{}
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, f := range files {
		log.Println("Reading File: ", f.Name())
		go func(f string) {
			wg.Add(1)
			defer wg.Done()
			// 打开文件，读取kv
			file, err := os.Open(filepath.Join("./mapdata/", f))
			if err != nil {
				log.Fatalf("cannot open %v", f)
			}
			dec := json.NewDecoder(file)
			var kv []mapreduceDemo.KV
			if err := dec.Decode(&kv); err != nil {
				log.Fatalf("cannot open %v", f)
			}
			mu.Lock()
			kva = append(kva, kv...)
			mu.Unlock()
		}(f.Name())
		// intermediate = append(intermediate, kva...)
	}
	wg.Wait()
	sort.Sort(KVSort(kva))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		// j 移动到下一个不同的key处
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		// 整合相同的key
		for k := i; k < j; k++ {
			values = append(values, strconv.Itoa(int(kva[k].Value.(float64))))
		}
		//output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		k := kva[i].Key
		v := values
		//fmt.Printf("%#v,  %#v\n", k, v)
		//新建一个ReduceTask
		task := mapreduceDemo.NewReduceTask(filepath.Join("./reducedata", "reduce_"+k+".txt"))
		go master.AddTask(&task, k, v)

		i = j
	}

	master.WaitReduceTask()
}
