package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

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

func execute(opts execOptions) {
	var clients []MQTT.Client
	for index := 0; index < opts.ClientNum; index++ {
		client := connect(index, opts)
		clients = append(clients, client)
	}
	asyncDisconnect(clients)
	time.Sleep(3)
}

func asyncDisconnect(clients []MQTT.Client) {
	wg := &sync.WaitGroup{}
	for _, c := range clients {
		wg.Add(1)
		go func(client MQTT.Client) {
			client.Disconnect(10)
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
	opts.SetCleanSession(false)

	client := MQTT.NewClient(opts)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	return client
}

func publishRequestAll(clients []MQTT.Client, opts execOptions, param ...string) int {
	//message := param[0]

	//wg := &sync.WaitGroup{}
	totalCount := 0
	return totalCount
}

func main() {
	execOpts := execOptions{}
	execOpts.Broker = "tcp://localhost:1883"
	execOpts.ClientNum = 10
	execute(execOpts)
}

/*
var rs1Letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

const rs2Letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

//define a function for the default message handler
var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

//use random
func initSeed() {
	rand.Seed(time.Now().UnixNano())
}

//cheak!! (シグナルを受け取るまで回し続けるっていうアイデア)
func periodPublish(c MQTT.Client) {
	for i := 0; i < 1; i++ {
		text := fmt.Sprintf("this is message")
		token := c.Publish("go-mqtt/sample", 0, false, text)
		token.Wait()
	}
}

//makeの理解がいまいち, もっと効率のいい生成があるっぽい.
func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = rs1Letters[rand.Intn(len(rs1Letters))]
	}
	return string(b)
}

func randomConnect(opts *MQTT.ClientOptions, i int, ch chan MQTT.Client) MQTT.Client {
	//randomInterval()
	clientID := "clientID: "
	clientID += strconv.FormatInt(int64(i), 10)
	opts.SetClientID(clientID)

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	//periodPublish(c)
	return c
}

func randomInterval() int {
	waitTime := rand.Intn(1000)
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
	return waitTime
}

func main() {
	initSeed()

	var clients []MQTT.Client
	clientsNum := 10
	ch := make(chan MQTT.Client)
	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetDefaultPublishHandler(f)
	opts.SetCleanSession(false)
	for i := 0; i < clientsNum; i++ {
		//clientID := randomString(10)
		c := randomConnect(opts, i, ch)
		clients = append(clients, c)
	}

	for index := 0; index < clientsNum; index++ {

	}

	for _, c := range clients {
		periodPublish(c)
		c.Disconnect(250)
		time.Sleep(300 * time.Millisecond)
	}
}
*/
// /usr/local/opt/mosquitto/sbin/mosquitto
/**

//subscribe to the topic /go-mqtt/sample and request messages to be delivered
//at a maximum qos of zero, wait for the receipt to confirm the subscription
if token := c.Subscribe("go-mqtt/sample", 0, nil); token.Wait() && token.Error() != nil {
	fmt.Println(token.Error())
	os.Exit(1)
}

//Publish 5 messages to /go-mqtt/sample at qos 1 and wait for the receipt
//from the server after sending each message
for i := 0; i < 5; i++ {
	text := fmt.Sprintf("this is msg #%d!", i)
	token := c.Publish("go-mqtt/sample", 0, false, text)
	token.Wait()
}

time.Sleep(3 * time.Second)

//unsubscribe from /go-mqtt/sample
if token := c.Unsubscribe("go-mqtt/sample"); token.Wait() && token.Error() != nil {
	fmt.Println(token.Error())
	os.Exit(1)
}

c.Disconnect(250)
*/
