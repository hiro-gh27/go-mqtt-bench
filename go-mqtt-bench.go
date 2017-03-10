package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var randSrc = rand.NewSource(time.Now().UnixNano())
var clientsHasErr = false

const (
	rs6Letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	rs6LetterIdxBits = 6
	rs6LetterIdxMask = 1<<rs6LetterIdxBits - 1
	rs6LetterIdxMax  = 63 / rs6LetterIdxBits
)

//使っていない項目多数あり...
type execOptions struct {
	Broker   string // Broker URI
	Qos      byte   // QoS(0|1|2)
	Retain   bool   // Retain
	Debug    bool   //デバック
	Topic    string // Topicのルート
	Username string // ユーザID
	Password string // パスワード
	//CertConfig        CertConfig // 認証定義
	ClientNum         int  // クライアントの同時実行数
	Count             int  // 1クライアント当たりのメッセージ数
	MessageSize       int  // 1メッセージのサイズ(byte)
	UseDefaultHandler bool // Subscriber個別ではなく、デフォルトのMessageHandlerを利用するかどうか
	PreTime           int  // 実行前の待機時間(ms)
	MaxInterval       int  // メッセージ毎の実行間隔時間(ms)
}

type clientResult struct {
	count int
}

func execute(exec func(clients []MQTT.Client, opts execOptions) int, opts execOptions) {
	rand.Seed(time.Now().UnixNano())
	var clients []MQTT.Client

	//slow
	if opts.Debug {
		hasErr := false
		for index := 0; index < opts.ClientNum; index++ {
			client := connect(index, opts)
			if client == nil {
				hasErr = true
				break
			}
			clients = append(clients, client)
		}

		if hasErr {
			for _, client := range clients {
				if client != nil {
					client.Disconnect(250)
				}
				fmt.Println("Connecting Error!!")
				return
			}
		}
		asyncDisconnect(clients)
		return
	}

	//fast
	clients = asynCconnectRequestAll(opts)

	if clientsHasErr {
		for _, client := range clients {
			if client != nil {
				client.Disconnect(250)
			}
		}
		fmt.Println("Connecting Error, exit program.")
		return
	}

	//wait a little to stability. 3000 ms is suitable :-)
	time.Sleep(3000 * time.Millisecond)

	startTime := time.Now()
	totalCount := exec(clients, opts)
	endTime := time.Now()

	duration := (endTime.Sub(startTime)).Nanoseconds() / int64(1000000) // nanosecond -> millisecond
	throughput := float64(totalCount) / float64(duration) * 1000        // messages/sec
	fmt.Printf("Result : broker=%s, clients=%d, totalCount=%d, duration=%dms, throughput=%.2fmessages/sec\n",
		opts.Broker, opts.ClientNum, totalCount, duration, throughput)

	asyncDisconnect(clients)
}

//同時にソケットを作ろうとするので, エラーが発生するかな?? 制限することで回避したつもりだが.
//たまにバーストする.
func asynCconnectRequestAll(execOpts execOptions) []MQTT.Client {
	wg := &sync.WaitGroup{}
	var clients []MQTT.Client
	socketToken := make(chan struct{}, 100) //並行にアクセスするクライアント数を制限
	for index := 0; index < execOpts.ClientNum; index++ {
		wg.Add(1)
		go func(id int) {
			prosessID := strconv.FormatInt(int64(os.Getpid()), 16)
			clientID := fmt.Sprintf("go-mqtt-bench%s-%d", prosessID, id)
			opts := MQTT.NewClientOptions()
			opts.AddBroker(execOpts.Broker)
			opts.SetClientID(clientID)
			client := MQTT.NewClient(opts)

			socketToken <- struct{}{}
			token := client.Connect()
			token.Wait()
			if token.Wait() && token.Error() != nil {
				fmt.Printf("Connected error: %s\n", token.Error())
				client = nil
				clientsHasErr = true
			}
			<-socketToken

			if execOpts.Debug {
				//fmt.Printf("connection clientID: %d\n", id)
			}
			clients = append(clients, client)
			wg.Done()
		}(index)
	}
	wg.Wait()
	return clients
}

//非同期で切断, たまにsoket errorになるけどなんで??
func asyncDisconnect(clients []MQTT.Client) {
	wg := &sync.WaitGroup{}
	for _, c := range clients {
		wg.Add(1)
		go func(client MQTT.Client) {
			client.Disconnect(250)
			wg.Done()
		}(c)
	}
	wg.Wait()
}

func asyncPublishAll(clients []MQTT.Client, opts execOptions) int {
	wg := &sync.WaitGroup{}
	var results []*clientResult
	var totalCount int
	massage := randomStr(opts.MessageSize)
	for id := 0; id < len(clients); id++ {
		wg.Add(1)
		c := clients[id]
		result := &clientResult{}
		results = append(results, result)
		go func(clientID int) {
			client := c
			for index := 0; index < opts.Count; index++ {
				//interval := rand.Intn(opts.MaxInterval)
				//time.Sleep(time.Duration(interval) * time.Millisecond)
				topic := fmt.Sprintf(opts.Topic+"%d", clientID)
				token := client.Publish(topic, opts.Qos, false, massage)
				result.count = result.count + 1
				if opts.Debug {
					fmt.Printf("Publish : id=%d, count=%d, topic=%s, massagesize=%v, \n", clientID, index, topic, len(massage))
					//fmt.Printf("Publish : id=%d, count=%d, topic=%s, interval=%d, massagesize=%v, \n", clientID, index, topic, interval, len(massage))
				}
				token.Wait()
			}
			wg.Done()
		}(id)
	}
	wg.Wait()

	//pub all counts.
	for _, val := range results {
		totalCount = totalCount + val.count
	}

	return totalCount
}

//スループットのためのstart時間が初subscribeの時間と異なる問題あり
func asyncSubscribeAll(clients []MQTT.Client, opts execOptions) int {
	wg := new(sync.WaitGroup)
	topic := fmt.Sprintf(opts.Topic + "#")
	var results []*clientResult
	for id := 0; id < len(clients); id++ {
		wg.Add(1)

		client := clients[id]
		result := &clientResult{}
		ch := make(chan bool)

		//call back func when massage arrive
		var handller MQTT.MessageHandler = func(client MQTT.Client, mag MQTT.Message) {
			result.count = result.count + 1
			ch <- true
			fmt.Print("now count is: ")
			fmt.Println(result.count)
		}

		token := client.Subscribe(topic, opts.Qos, handller)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("Subscribe error: %s\n", token.Error())
		}
		results = append(results, result)

		//all Subscriber wait "opts.Count" massage, but less massege can't unlock....
		go func() {
			for index := 0; index < opts.Count; index++ {
				<-ch
			}
			close(ch)
			wg.Done()
		}()
	}
	wg.Wait()

	//sub all counts.
	var totalCount int
	for _, val := range results {
		totalCount = totalCount + val.count
	}

	return totalCount
}

//go funcにすると, もう少し早くなるかと...
func connect(id int, execOpts execOptions) MQTT.Client {
	//clientID is prosessID and index
	prosessID := strconv.FormatInt(int64(os.Getpid()), 16)
	clientID := fmt.Sprintf("go-mqtt-bench%s-%d", prosessID, id)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(execOpts.Broker)
	opts.SetClientID(clientID)
	//opts.SetCleanSession(false)

	client := MQTT.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Connected error: %s\n", token.Error())
		return nil
	}
	return client
}

//this random strings is very fast!!
//defaul string is utf-8, so 1 character equal 8bit -> 1byte
//look >> http://qiita.com/srtkkou/items/ccbddc881d6f3549baf1
func randomStr(n int) string {
	b := make([]byte, n)
	cache, remain := randSrc.Int63(), rs6LetterIdxMax
	for i := n - 1; i >= 0; {
		if remain == 0 {
			cache, remain = randSrc.Int63(), rs6LetterIdxMax
		}
		idx := int(cache & rs6LetterIdxMask)
		if idx < len(rs6Letters) {
			b[i] = rs6Letters[idx]
			i--
		}
		cache >>= rs6LetterIdxBits
		remain--
	}
	return string(b)
}

//コマンドラインから指定できると, もっとエレガントなプログラムになるのだが...
func main() {
	//use max cpu
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	execOpts := execOptions{}
	execOpts.Broker = "tcp://169.254.120.135:1883" // this is my second pc Address
	//execOpts.Broker = "tcp://localhost:1883"
	execOpts.ClientNum = 2000
	execOpts.Qos = 0
	execOpts.Count = 10
	execOpts.Topic = "go-mqtt/"
	execOpts.MaxInterval = 0
	execOpts.MessageSize = 100

	execOpts.Debug = false

	method := "pub"
	switch method {
	case "pub":
		execute(asyncPublishAll, execOpts)
	case "sub":
		execute(asyncSubscribeAll, execOpts)
	}

}
