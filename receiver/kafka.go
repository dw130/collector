package receiver

import (
	cluster "github.com/bsm/sarama-cluster"
	"fmt"
	"log"
	"github.com/collector/filter"
	"github.com/collector/stat"
	"github.com/collector/conf"
)

type Kafka struct {
	brokerList []string
	config     *cluster.Config
	consumer   *cluster.Consumer
	stop       chan bool
	filter     *filter.Filter
	input     *stat.Static
}


func NewKafkaIns(c *conf.Config, filter  *filter.Filter) *Kafka{

	inputConf := c.Input

	k := &Kafka{}
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	k.config = config
	k.stop = make(chan bool)
	k.filter = filter

	k.input = stat.NewStatic( fmt.Sprintf("kafka_%v", inputConf.Name ))

	consumer, err := cluster.NewConsumer(inputConf.BrokerList, "devops",inputConf.Topic, config)
	if err != nil {
		fmt.Printf("new consumer fail:%v\n",err)
		return nil
	}

	k.consumer = consumer

	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	go k.Consumer()
	log.Printf("new kafka:%+v",inputConf)
	return k
}

func (k *Kafka) Consumer() {

	for {
		select {
		case msg, ok := <- k.consumer.Messages():
			if ok {
				k.input.Add(1)
				k.filter.Input(msg.Value)
			}
			k.consumer.MarkOffset(msg, "")
		case <- k.stop:
			return
		}
	}
}

func (k *Kafka) Close() {
	k.consumer.Close()
	k.stop <- true
}


