package kafka

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
//	"log"
//	"os"

	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/logp"
	"github.com/elastic/libbeat/outputs"

	"github.com/Shopify/sarama"
)

type KafkaDataType uint16

const (
	KafkaListType KafkaDataType = iota
	KafkaChannelType
)

type KafkaOutput struct {
	ReconnectInterval  time.Duration
	BrokerList         []string
	Topic              string
	Timeout            time.Duration
	FlushInterval      time.Duration
	Producer           sarama.AsyncProducer
	sendingQueue chan KafkaQueueMsg
	connected    bool
}

type KafkaQueueMsg struct {
	msg   []byte
}

func (qmsg *KafkaQueueMsg) Length() int {
	return len(qmsg.msg)
}

func (qmsg *KafkaQueueMsg) Encode() ([]byte, error) {
	return qmsg.msg, nil
}

func (out *KafkaOutput) Init(config outputs.MothershipConfig, topology_expire int) error {

	if config.Host == "" {
		return errors.New("No Kafka brokers specified")
	}
	out.BrokerList = strings.Split(config.Host, ",")

	if config.Topic == "" {
		return errors.New("No Kafka topic specified")
	}
	out.Topic = config.Topic

	out.Timeout = 5 * time.Second
	if config.Timeout != 0 {
		out.Timeout = time.Duration(config.Timeout) * time.Second
	}

	out.FlushInterval = 1000 * time.Millisecond
	out.ReconnectInterval = time.Duration(1) * time.Second
	if config.Reconnect_interval != 0 {
		out.ReconnectInterval = time.Duration(config.Reconnect_interval) * time.Second
	}

	//sarama.Logger = log.New(os.Stdout, "[KafkaOutput]", log.LstdFlags)
	logp.Info("[KafkaOutput] Using Kafka brokers %s", config.Host)
	logp.Info("[KafkaOutput] Kafka connection timeout %s", out.Timeout)
	logp.Info("[KafkaOutput] Kafka reconnect interval %s", out.ReconnectInterval)
	logp.Info("[KafkaOutput] Kafka flushing interval %s", out.FlushInterval)
	logp.Info("[KafkaOutput] Publishing to topic %s", out.Topic)

	out.sendingQueue = make(chan KafkaQueueMsg, 1000)

	out.Reconnect()
	go out.SendMessagesGoroutine()

	return nil
}

func (out *KafkaOutput) newProducer() (sarama.AsyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency =  out.FlushInterval
	config.Producer.Return.Errors = true
	config.Net.DialTimeout = out.Timeout
	config.Net.ReadTimeout = out.Timeout
	config.Net.WriteTimeout = out.Timeout

	producer, err := sarama.NewAsyncProducer(out.BrokerList, config)
	if err != nil {
		logp.Err("Failed to start Sarama producer: %s", err)
		return nil, err
	}

	return producer, nil
}

func (out *KafkaOutput) Connect() error {
	var err error
	out.Producer, err = out.newProducer()
	if err != nil {
		return err
	}
	out.connected = true

	return nil
}

func (out *KafkaOutput) Close() {
	out.Producer.Close()
}

func (out *KafkaOutput) SendMessagesGoroutine() {

	for {
		select {

		case queueMsg := <-out.sendingQueue:

			if !out.connected {
				logp.Debug("output_kafka", "Droping pkt ...")
				continue
			}
			logp.Debug("output_kafka", "Send event to kafka")

			out.Producer.Input() <- &sarama.ProducerMessage {
				Topic: out.Topic,
				Key: nil,
				Value: &queueMsg,
			}

 		case err := <- out.Producer.Errors():
			logp.Err("Failed to publish event to kafka: %s", err)
			out.connected = false
			out.Close()
			go out.Reconnect()
			return
		}
	}
}

func (out *KafkaOutput) Reconnect() {
	for {
		err := out.Connect()
		if err != nil {
			logp.Warn("Error connecting to Kafka (%s). Retrying in %s", err, out.ReconnectInterval)
			time.Sleep(out.ReconnectInterval)
		} else {
			break
		}
	}
}

func (out *KafkaOutput) GetNameByIP(ip string) string {
	//NOT SUPPORTED
	return ""
}

func (out *KafkaOutput) PublishIPs(name string, localAddrs []string) error {
	//NOT SUPPORTED
	return nil
}

func (out *KafkaOutput) PublishEvent(ts time.Time, event common.MapStr) error {

	json_event, err := json.Marshal(event)
	if err != nil {
		logp.Err("Failed to convert the event to JSON: %s", err)
		return err
	}

	out.sendingQueue <- KafkaQueueMsg{msg: json_event}

	logp.Debug("output_kafka", "Publish event")
	return nil
}
