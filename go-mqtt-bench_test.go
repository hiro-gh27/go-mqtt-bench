package main

import (
	"sync"
	"testing"
)

func BenchmarkRandomStr(b *testing.B) {
	for index := 0; index < b.N; index++ {
		randomStr2(1000)
	}
}

//for async access
func TestRandomStr2(t *testing.T) {
	var messages []string
	var strLen int

	strLen = 1000

	wg := new(sync.WaitGroup)
	for index := 0; index < 50000; index++ {
		wg.Add(1)
		go func() {
			str := randomStr2(strLen)
			messages = append(messages, str)
			wg.Done()
		}()
	}
	wg.Wait()
}
