package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"sort"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var randSrc = rand.NewSource(time.Now().UnixNano())
var clientsHasErr = false

//use randomMessage
const (
	letters       = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
)

//use randomStr
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
	Method   string // 実行メソッド
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
	time  []time.Time
}

/**
 * 実行し, スループットの計算をする.
 */
func execute(exec func(clients []MQTT.Client, opts execOptions), opts execOptions) {
	rand.Seed(time.Now().UnixNano())
	//var clients []MQTT.Client

	clients, times := asynCconnectRequestAll(opts)

	if len(clients) < opts.ClientNum {
		fmt.Println("========= Error!! Disconnect and Exit ==========")
		fmt.Printf("clients: %d, opts.ClienNum: %d\n", len(clients), opts.ClientNum)
		asyncDisconnect(clients)
		return
	}

	connectThoroughput(times)

	//wait a little to stability. 3000 ms is suitable :-)
	time.Sleep(3000 * time.Millisecond)

	exec(clients, opts)

	asyncDisconnect(clients)
}

/**
 * 非同期でクライアント作成と接続を行う.
 */
func asynCconnectRequestAll(execOpts execOptions) ([]MQTT.Client, []time.Time) {
	wg := &sync.WaitGroup{}
	var clients []MQTT.Client
	var times []time.Time
	socketToken := make(chan struct{}, 100) //並行にアクセスするクライアント数を制限, 多すぎるとSYN/ACK待ちソケット数の制限に引っかかる
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
			<-socketToken

			if token.Wait() && token.Error() != nil {
				fmt.Printf("Connected error: %s\n", token.Error())
			} else {
				time := time.Now()
				clients = append(clients, client)
				times = append(times, time)
			}
			if execOpts.Debug {
				fmt.Printf("connection clientID: %d\n", id)
			}
			wg.Done()
		}(index)
	}
	wg.Wait()

	return clients, times
}

/**
 * 非同期で切断を行う.
 */
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

/**
 * 非同期でPublishをそれぞれのクライアントが行う.
 */
func asyncPublishAll(clients []MQTT.Client, opts execOptions) {
	wg := &sync.WaitGroup{}
	startTime := time.Now()
	var results []*clientResult

	for id := 0; id < len(clients); id++ {
		wg.Add(1)
		c := clients[id]
		result := &clientResult{}
		results = append(results, result)
		/**
		 * go func that do async Publish
		 */
		go func(clientID int) {
			client := c
			for index := 0; index < opts.Count; index++ {
				massage := randomMessage(opts.MessageSize)
				topic := fmt.Sprintf(opts.Topic+"%d", clientID)
				if opts.MaxInterval > 0 {
					interval := rand.Intn(opts.MaxInterval)
					time.Sleep(time.Duration(interval) * time.Millisecond)
				}
				token := client.Publish(topic, opts.Qos, false, massage)
				token.Wait()
				result.time = append(result.time, time.Now())
				if opts.Debug {
					fmt.Printf("Publish : id=%d, count=%d, topic=%s, massagesize=%v, \n", clientID, index, topic, len(massage))
				}
			}
			wg.Done()
		}(id)

	}
	wg.Wait()

	//スループットの手続き
	var times []time.Time
	times = append(times, startTime)
	for _, val := range results {
		for _, time := range val.time {
			times = append(times, time)
		}
	}
	pubsubThoroughput(times, opts.Method)
}

/**
 * 非同期でsubscribeを行う.
 */
func asyncSubscribeAll(clients []MQTT.Client, opts execOptions) {
	wg := new(sync.WaitGroup)
	topic := fmt.Sprintf(opts.Topic + "#")
	var results []*clientResult

	for id := 0; id < len(clients); id++ {
		wg.Add(1)
		client := clients[id]
		result := &clientResult{}
		ch := make(chan bool)

		/**
		 * callBack function
		 */
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
		/**
		 * go function
		 */
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

}

// 遊びですべ

/**
 * Brokerに順次接続する. 並行処理版を作成したので, そちらを利用中
 */
func connect(id int, execOpts execOptions) MQTT.Client {
	prosessID := strconv.FormatInt(int64(os.Getpid()), 16)
	clientID := fmt.Sprintf("go-mqtt-bench%s-%d", prosessID, id)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(execOpts.Broker)
	opts.SetClientID(clientID)
	client := MQTT.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Connected error: %s\n", token.Error())
		return nil
	}
	return client
}

/**
 * 高速にnバイト文字列を生成する. 同時並行で使えないので利用停止.
 */
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

/**
 * nバイト文字列を生成する. 同時並行でも使える.
 */
func randomMessage(n int) string {
	message := make([]byte, n)
	for i := 0; i < n; {
		index := int(rand.Int63() & letterIdxMask)
		if index < len(letters) {
			message[i] = letters[index]
			i++
		}
	}
	return string(message)
}

/**
 * 応答時間を求める.
 */
func singlePubSub(clients []MQTT.Client, opts execOptions) {
}

/**
 * タイムスライスからスループットを求める.
 */
func connectThoroughput(times []time.Time) {
	totalCount := len(times)
	startTime := times[0]
	endTime := times[totalCount-1]
	test := endTime.Sub(startTime).Nanoseconds()
	fmt.Println(test)
	duration := (endTime.Sub(startTime)).Nanoseconds() / int64(1000000) // nanosecond -> millisecond
	throughput := float64(totalCount) / float64(duration) * 1000        // messages/sec
	fmt.Printf("コネクションスループット : totalCount=%d, duration=%dms, throughput=%.2fmessages/sec\n",
		totalCount, duration, throughput)
}

/**
 * 未sortのタイムスライスから, スループットを求める.
 */
func pubsubThoroughput(times []time.Time, method string) {
	totalCount := len(times) - 1
	var intTimes []int
	for _, t := range times {
		intTime := t.Minute()*60*1000000000 + t.Second()*1000000000 + t.Nanosecond()
		intTimes = append(intTimes, intTime)
	}
	sort.Sort(sort.IntSlice(intTimes))
	startTime := intTimes[0]
	endTime := intTimes[totalCount]
	duration := (endTime - startTime) / 1000000                  // nanosecond -> millisecond
	throughput := float64(totalCount) / float64(duration) * 1000 // messages/sec
	fmt.Printf("%sスループット : totalCount=%d, duration=%dms, throughput=%.2fmessages/sec\n",
		method, totalCount, duration, throughput)
}

//コマンドラインから指定できると, もっとエレガントなプログラムになるのだが...
func main() {
	/*
		use max cpu
		cpus := runtime.NumCPU()
		runtime.GOMAXPROCS(cpus)
	*/

	execOpts := execOptions{}
	//execOpts.Broker = "tcp://169.254.120.135:1883"
	execOpts.Broker = "tcp://localhost:1883"
	execOpts.ClientNum = 10
	execOpts.Qos = 0
	execOpts.Count = 1
	execOpts.Topic = "go-mqtt/"
	execOpts.MaxInterval = 1000
	execOpts.MessageSize = 100

	execOpts.Debug = false

	execOpts.Method = "pub"
	switch execOpts.Method {
	case "pub":
		execute(asyncPublishAll, execOpts)
	case "sub":
		execute(asyncSubscribeAll, execOpts)
	case "singlePubSub":
		execOpts.ClientNum = 2
		execute(singlePubSub, execOpts)
	}

}
