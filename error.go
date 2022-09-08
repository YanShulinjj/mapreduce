/* ----------------------------------
*  @author suyame 2022-08-31 15:52:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package mapreduceDemo

import "errors"

var (
	TaskTypeErr        = errors.New("Unknown task type!")
	TaskCrashErr       = errors.New("Task in worker doesnt done yet!")
	WokerReqTypeErr    = errors.New("Request for worker is not correct!")
	TaskNotResigterErr = errors.New("No task in worker!")
	TaskNotFoundErr    = errors.New("No such task in system.")
)
