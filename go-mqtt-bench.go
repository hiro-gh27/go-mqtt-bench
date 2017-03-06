package main

import (
	"fmt"
	"math/rand"
	"time"

	"strconv"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

//define a function for the default message handler
var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

var rs1Letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

const rs2Letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	initSeed()

	//var clients [10]MQTT.Client
	//create a ClientOptions struct setting the broker address, clientid, turn
	//off trace output and set the default message handler
	opts := MQTT.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetDefaultPublishHandler(f)
	opts.SetCleanSession(false)
	for i := 0; i < 10; i++ {
		//clientID := randomString(10)
		go randomConnect(opts, i)

	}

	time.Sleep(10000 * time.Millisecond)

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
}

//use random
func initSeed() {
	rand.Seed(time.Now().UnixNano())
}

//cheak!! (シグナルを受け取るまで回し続けるっていうアイデア)
func periodPublish(c MQTT.Client) {
	for {
		text := fmt.Sprintf("this is message")
		token := c.Publish("go-mqtt/sample", 0, false, text)
		token.Wait()
	}
	/**
	for i := 0; i < 1; i++ {
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish("go-mqtt/sample", 0, false, text)
		token.Wait()
	}
	*/
}

//makeの理解がいまいち, もっと効率のいい生成があるっぽい.
func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = rs1Letters[rand.Intn(len(rs1Letters))]
	}
	return string(b)
}

func randomConnect(opts *MQTT.ClientOptions, i int) MQTT.Client {
	waitTime := randomInterval()
	clientID := "clientID: "
	clientID += strconv.FormatInt(int64(i), 10)
	clientID += " / waitTime= "
	clientID += strconv.FormatInt(int64(waitTime), 10)
	opts.SetClientID(clientID)

	//create and start a client using the above ClientOptions
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

// /usr/local/opt/mosquitto/sbin/mosquitto
