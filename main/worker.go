/* ----------------------------------
*  @author suyame 2022-08-31 15:42:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package main

import (
	"fmt"
	"mapreduceDemo"
	"os"
	"time"
)

func main() {
	fmt.Println(os.Args[1])
	w := mapreduceDemo.NewWorker(os.Args[1])
	for w.IsAlive() {
		time.Sleep(time.Second)
	}
}
