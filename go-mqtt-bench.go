package main

import (
	"bytes"
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

const (
	rs6Letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	rs6LetterIdxBits = 6
	rs6LetterIdxMask = 1<<rs6LetterIdxBits - 1
	rs6LetterIdxMax  = 63 / rs6LetterIdxBits
)

//使っていないstructいっぱいだお
type execOptions struct {
	Broker   string // Broker URI
	Qos      byte   // QoS(0|1|2)
	Retain   bool   // Retain
	Topic    string // Topicのルート
	Username string // ユーザID
	Password string // パスワード
	//CertConfig        CertConfig // 認証定義
	ClientNum         int  // クライアントの同時実行数
	Count             int  // 1クライアント当たりのメッセージ数
	MessageSize       int  // 1メッセージのサイズ(byte)
	UseDefaultHandler bool // Subscriber個別ではなく、デフォルトのMessageHandlerを利用するかどうか
	PreTime           int  // 実行前の待機時間(ms)
	IntervalTime      int  // メッセージ毎の実行間隔時間(ms)
}

type clientResult struct {
	count int
}

func execute(opts execOptions) {
	var clients []MQTT.Client
	for index := 0; index < opts.ClientNum; index++ {
		client := connect(index, opts)
		clients = append(clients, client)
	}
	//安定せるために少し待つらしい. 3000は適当です :-)
	time.Sleep(3000 * time.Millisecond)

	startTime := time.Now()
	fmt.Println("start")
	//publishRequestAll(clients, opts)
	subscribeRequestAll(clients, opts)
	endTime := time.Now()
	duration := (endTime.Sub(startTime)).Nanoseconds() / int64(1000000)
	fmt.Println(duration)

	asyncDisconnect(clients)
}

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

func connect(id int, execOpts execOptions) MQTT.Client {
	prosessID := strconv.FormatInt(int64(os.Getpid()), 16)
	clientID := fmt.Sprintf("go-mqtt-bench%s-%d", prosessID, id)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(execOpts.Broker)
	opts.SetClientID(clientID)
	//opts.SetCleanSession(true)

	client := MQTT.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Connected error: %s\n", token.Error())
	}
	return client
}

func createFixedSizeMassage(size int) string {
	var buffer bytes.Buffer
	for index := 0; index < size; index++ {
		buffer.WriteString(strconv.Itoa(index % 10))
	}
	massage := buffer.String()
	return massage
}

func publishRequestAll(clients []MQTT.Client, opts execOptions) int {
	wg := &sync.WaitGroup{}
	totalCount := 0
	for id := 0; id < len(clients); id++ {
		wg.Add(1)
		c := clients[id]
		go func(clientID int) {
			client := c
			for index := 0; index < opts.Count; index++ {
				massage := randomStr(100)
				fmt.Printf("massagesize= %v\n", len(massage))
				topic := fmt.Sprintf(opts.Topic+"%d", clientID)
				//message := createFixedSizeMassage(100)
				//token := client.Publish(topic, opts.Qos, false, fmt.Sprintf("%d", clientID))
				token := client.Publish(topic, opts.Qos, false, massage)
				//fmt.Printf("Publish : id=%d, count=%d, topic=%s\n", clientID, index, topic)
				token.Wait()
			}
			sys := fmt.Sprintf("finished ClientID = %d", clientID)
			fmt.Println(sys)
			wg.Done()
		}(id)
	}
	wg.Wait()
	//オリジナルでは,totalCountを取得していたけれども, 独立したスレッドから共有資源へのアクセスは複雑になると思われ.
	//しかし, そんな操作は見当たらず...うむ....
	//subscribe同様に, ポインタをうまく活用するとtotalcontuできる模様
	return totalCount
}

//現在理解進行中(途中までは理解できた), これがかなり高速らしい. 標準でutf-8 -> ascii=1バイト
//参考 -> http://qiita.com/srtkkou/items/ccbddc881d6f3549baf1
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

func subscribeRequestAll(clients []MQTT.Client, opts execOptions) {
	//wg := new(sync.WaitGroup)
	topic := fmt.Sprintf(opts.Topic + "#")
	var results []*clientResult
	for id := 0; id < len(clients); id++ {
		//wg.Add(1)

		client := clients[id]
		result := &clientResult{}
		//init is need or not??
		//result.count = 0
		var handller MQTT.MessageHandler = func(client MQTT.Client, mag MQTT.Message) {
			result.count = result.count + 1
			fmt.Print("now count is: ")
			fmt.Println(result.count)
		}
		token := client.Subscribe(topic, opts.Qos, handller)
		if token.Wait() && token.Error() != nil {
			fmt.Printf("Subscribe error: %s\n", token.Error())
		}
		results = append(results, result)
	}
	time.Sleep(20 * time.Second)

	var totalCount int
	for _, val := range results {
		totalCount = totalCount + val.count
	}
	fmt.Print("total count is: ")
	fmt.Println(totalCount)

}

func main() {
	//デフォルトでcpuは4つ使うのかな...??
	cpus := runtime.NumCPU()
	println(cpus)
	runtime.GOMAXPROCS(cpus)
	execOpts := execOptions{}
	execOpts.Broker = "tcp://169.254.120.135:1883"
	execOpts.ClientNum = 5
	execOpts.Qos = 0
	execOpts.Count = 100
	execOpts.Topic = "go-mqtt/"
	execute(execOpts)

}
