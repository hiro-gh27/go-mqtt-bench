package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

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
	Broker            string // Broker URI
	Qos               byte   // QoS(0|1|2)
	Retain            bool   // Retain
	Debug             bool   //デバック
	Topic             string // Topicのルート
	Username          string // ユーザID
	Password          string // パスワード
	Method            string // 実行メソッド
	ClientNum         int    // クライアントの同時実行数
	Count             int    // 1クライアント当たりのメッセージ数
	MessageSize       int    // 1メッセージのサイズ(byte)
	UseDefaultHandler bool   // Subscriber個別ではなく、デフォルトのMessageHandlerを利用するかどうか
	PreTime           int    // 実行前の待機時間(ms)
	MaxInterval       int    // メッセージ毎の実行間隔時間(ms)
	test              bool   //テスト
	trialNum          int    //試行回数
}

type clientResult struct {
	count int
	times []time.Time
	time  time.Time
	id    int
}

/**
 * 実行し, スループットの計算をする.
 */
func execute(exec func(clients []MQTT.Client, opts execOptions), opts execOptions) {
	rand.Seed(time.Now().UnixNano())
	//var clients []MQTT.Client
	var testclinets []MQTT.Client
	if opts.test {
		for index := 0; index < opts.ClientNum; index++ {
			client := connect(index, opts)
			testclinets = append(testclinets, client)
		}
		//singlePubSub(testclinets, opts)
		return
	}

	clients, times := asynCconnectRequestAll(opts)
	if clientsHasErr == true {
		fmt.Println("========= Error!! Disconnect and Exit programs ==========")
		for index := 0; index < len(clients); index++ {
			client := clients[index]
			if client != nil {
				client.Disconnect(250)
			}
		}
		return
	}

	if len(clients) < opts.ClientNum {
		fmt.Println("========= Error!! Disconnect and Exit ==========")
		fmt.Printf("clients: %d, opts.ClienNum: %d\n", len(clients), opts.ClientNum)
		asyncDisconnect(clients)
		return
	}
	thoroughputCalc(times, "コネクション")
	//wait a little to stability. 3000 ms is suitable :-)
	time.Sleep(3000 * time.Millisecond)
	exec(clients, opts)
	asyncDisconnect(clients)
}

/**
 * 非同期でクライアント作成と接続を行う.
 * クライアント数を合わせるために, 無茶してるので要修正
 */
func asynCconnectRequestAll(execOpts execOptions) ([]MQTT.Client, []time.Time) {
	wg := &sync.WaitGroup{}
	clients := make([]MQTT.Client, execOpts.ClientNum)
	times := make([]time.Time, execOpts.ClientNum)
	socketToken := make(chan struct{}, 100) //並列で処理を進めたいのだが, ソケット数に規制が存在するため...
	startTime := time.Now()
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
				clientsHasErr = true
				clients[id] = nil
			} else {
				times[id] = time.Now()
				clients[id] = client
			}
			if execOpts.Debug {
				fmt.Printf("connection clientID: %d\n", id)
			}
			wg.Done()
		}(index)
	}
	wg.Wait()
	times = append(times, startTime)
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
	for index := 0; index < opts.trialNum; index++ {
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
					result.times = append(result.times, time.Now())
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
			for _, time := range val.times {
				times = append(times, time)
			}
		}
		thoroughputCalc(times, opts.Method)
		time.Sleep(500 * time.Millisecond)
	}
}

/**
 * 非同期でsubscribeを行う. brokerの配送スループットが計測できる.
 */
func asyncSubscribeAll(clients []MQTT.Client, opts execOptions) {
	wg := new(sync.WaitGroup)
	publisher := clients[len(clients)-1]
	clients = clients[:len(clients)-1]
	var results []*clientResult
	for index := 0; index < len(clients); index++ {
		client := clients[index]
		result := &clientResult{}
		results = append(results, result)
		result.id = index
		singleSubscribe(wg, result, opts, client)
		time.Sleep(300 * time.Millisecond)
	}
	// * do 1publish and wait to arrive message.
	for index := 0; index < opts.trialNum; index++ {
		wg.Add(len(clients))
		var times []time.Time
		startTime := singlePublish(opts, publisher, index)
		wg.Wait()
		times = append(times, startTime)
		for _, val := range results {
			times = append(times, val.time)
		}
		thoroughputCalc(times, opts.Method)
		time.Sleep(1000 * time.Millisecond)
	}
}

/*
 * 1subscribeを実行する. 引数が多いのがイマイチ... ロジックも複雑すぎるか!?
 */
func singleSubscribe(wg *sync.WaitGroup, result *clientResult, opts execOptions, client MQTT.Client) {
	opts.Debug = true
	topic := fmt.Sprintf(opts.Topic + "#")
	var handller MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
		if opts.Debug {
			fmt.Printf("clientID= %d, topic= %s\n", result.id, msg.Topic())
		}
		result.time = time.Now()
		wg.Done()
	}
	token := client.Subscribe(topic, opts.Qos, handller)
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Subscribe error: %s\n", token.Error())
	}
}

/*
 * 1回だけpublishを行う. indexは何度目の試行なのかを示している.
 */
func singlePublish(opts execOptions, publisher MQTT.Client, index int) time.Time {
	pubTopic := fmt.Sprintf(opts.Topic+"trial-%d", index)
	massage := randomMessage(opts.MessageSize)
	token := publisher.Publish(pubTopic, opts.Qos, false, massage)
	token.Wait()
	finTime := time.Now()
	if token.Wait() && token.Error() != nil {
		fmt.Printf("Publish error: %s\n", token.Error())
	}
	return finTime
}

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
	time.Sleep(100 * time.Millisecond)
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
func thoroughputCalc(times []time.Time, method string) {
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
	//use max cpu
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	runtime.GOMAXPROCS(1)
	execOpts := execOptions{}
	execOpts.Broker = "tcp://169.254.120.135:1883"
	//execOpts.Broker = "tcp://localhost:1883"
	execOpts.Broker = "tcp://192.168.56.101:1883"
	execOpts.Broker = "tcp://192.168.56.102:1883"
	execOpts.ClientNum = 500
	execOpts.Qos = 0
	execOpts.Count = 10
	execOpts.trialNum = 5
	execOpts.Topic = "go-mqtt/"
	execOpts.MaxInterval = 0
	execOpts.MessageSize = 1000

	execOpts.Debug = false
	execOpts.test = false

	execOpts.Method = "singlePubSub"
	switch execOpts.Method {
	case "pub":
		execute(asyncPublishAll, execOpts)
	case "sub":
		execOpts.ClientNum = execOpts.ClientNum + 1 // 1client will be publisher.
		execute(asyncSubscribeAll, execOpts)
	case "singlePubSub":
		execOpts.ClientNum = 2
		execute(asyncSubscribeAll, execOpts)
	}

}

/* TODO
-

- subscribeの時間がちゃんと取れているのか

- connectスループットの計算が少しおかしいかもね

- コマンドラインで受付けるように変更
> serverOSでプログラム修正が容易ではないため, コマンドラインでの入力が望ましいとわかった.

- 基本appendをあやしむ

- subscribeタイムアウトを設定しないと, どっかでロストする確率高すぎて.
> タイムアウト処理のプログラム作成コストが高すぎると判断

 * 応答時間を求める.
func singlePubSub(clients []MQTT.Client, opts execOptions) {
	wg := new(sync.WaitGroup)
	publisher := clients[len(clients)-1]
	clients = clients[:len(clients)-1]
	var results []*clientResult
	for index := 0; index < len(clients); index++ {
		client := clients[index]
		result := &clientResult{}
		results = append(results, result)
		result.id = index
		singleSubscribe(wg, result, opts, client)
		time.Sleep(300 * time.Millisecond)
	}

	// * do 1publish and wait to arrive message.
	for index := 0; index < opts.trialNum; index++ {
		wg.Add(len(clients))
		var times []time.Time
		startTime := singlePublish(opts, publisher, index)
		wg.Wait()
		times = append(times, startTime)
		for _, val := range results {
			times = append(times, val.time)
		}
		thoroughputCalc(times, opts.Method)
		time.Sleep(1000 * time.Millisecond)
	}
}


*/
