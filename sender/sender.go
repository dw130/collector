package sender

import (
	"github.com/collector/conf"
	client "github.com/influxdata/influxdb/client/v2"
)

type Sender interface {
	Close()
	Convert([]byte)
	Init(conf.OutPutConf)
	Type() string
	Add(p *client.Point)
}


var (
	Mapping = map[string] Sender{}
)


func Reg(key string,s Sender) {
	Mapping[key] = s
}


func FetchSenders(c *conf.Config) []Sender {

	instanceList := []Sender{}

	for k,_ := range c.Output {
		singleConf := c.Output[k]
		outType := singleConf.OutType
		instance,ok := Mapping[outType]
		if ok == false {
			continue
		}

		instance.Init(singleConf)
		instanceList = append(instanceList, instance)
	}
	return instanceList
}


	//influxdbAddr := "10.0.52.130:9000"
	//influx := sender.NewInflux(influxdbAddr,SENDER_WORKER)
	//senderList := []sender.Sender{}
	//senderList = append(senderList,influx)