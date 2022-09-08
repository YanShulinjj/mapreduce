/* ----------------------------------
*  @author suyame 2022-08-30 20:41:00
*  Crazy for Golang !!!
*  IDE: GoLand
*-----------------------------------*/

package mapreduceDemo

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

func Map(k, v interface{}) []KV {
	filename := v.(string)
	file, err := os.Open(filepath.Join("./data", filename))
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file) // content为文件内容
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 切分content
	f := func(r rune) bool { return !unicode.IsLetter(r) }
	// 以不是字母的字符初进行切分
	words := strings.FieldsFunc(string(content), f)
	// 遍历每个word
	kva := []KV{}
	for _, w := range words {
		kv := KV{Key: w, Value: 1}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(k, v interface{}) string {
	ws := v.([]string)
	return strconv.Itoa(len(ws))
}
