package main

import (
	"sync"
	"testing"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func BenchmarkRandomStr(b *testing.B) {
	for index := 0; index < b.N; index++ {
		randomStr2(1000)
	}
}

func BenchmarkConnect(b *testing.B) {
	var clients []MQTT.Client
	doAppend := true
	opts := initOptions()

	b.ResetTimer()
	if doAppend {
		for index := 0; index < b.N; index++ {
			client := connect(index, opts)
			clients = append(clients, client)
		}
	} else {
		clients = make([]MQTT.Client, 1000)
		for index := 0; index < b.N; index++ {
			client := connect(index, opts)
			clients[index] = client
		}
	}
	b.StopTimer()

	var rclients []MQTT.Client
	if doAppend {
		rclients = clients
	} else {
		for index := 0; index < len(clients); index++ {
			if clients[index] != nil {
				rclients = append(rclients, clients[index])
			}
		}
	}
	asyncDisconnect(rclients)
}

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

func initOptions() execOptions {
	execOpts := execOptions{}
	execOpts.Broker = "tcp://localhost:1883"
	execOpts.Qos = 0
	execOpts.Retain = false
	execOpts.Topic = "go-mqtt-go/"
	execOpts.MessageSize = 1024
	execOpts.ClientNum = 100
	execOpts.Count = 5
	execOpts.MaxInterval = 0
	execOpts.sleepTime = 0
	execOpts.trialNum = 10
	execOpts.synBacklog = 128

	execOpts.Method = "Benchmark"

	return execOpts
}
