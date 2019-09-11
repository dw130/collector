package filter


import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/collector/stat"
	"github.com/collector/conf"
	"github.com/collector/sender"
	"github.com/collector/model"
	"github.com/collector/model/oplogManagerPB"
	"github.com/golang/protobuf/proto"
	"github.com/garyburd/redigo/redis"
)

var (
  _ = fmt.Sprintf("")
)

type Filter struct {
	senderList []sender.Sender
	conf       conf.FilterConf
	input      *stat.Static
	data       chan []byte
	stop       chan bool
	ctx        context.Context
	cancel     context.CancelFunc
	model      model.General
	redisIns   *redis.Pool
}

func NewRedisPoos(host string,pass string) *redis.Pool {

	RedisClient := &redis.Pool{
		MaxIdle:     1,
		MaxActive:   60,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", pass); err != nil {
				c.Close()
				return nil, err
			}

			c.Do("SELECT", 0)
			return c, nil
		},
	}
	return RedisClient
}

func NewFilter(c *conf.Config,senderList []sender.Sender) *Filter {
	f := &Filter{}
	f.senderList = senderList
	f.conf = c.Filter
	f.data = make(chan []byte,100000)
	f.stop = make(chan bool)
	//f.input = stat.NewStatic("input:filter")
	f.ctx, f.cancel = context.WithCancel(context.Background())

	f.redisIns = NewRedisPoos(f.conf.RedisHost, f.conf.RedisPass)

	f.model = model.FetchInstance(&f.conf,f.redisIns)

	for k:=0;k<f.conf.Worker;k++ {
		go f.Run(k)
	}
	log.Printf( "new filter:%+v,model:%T",f.conf,f.model )

	return f
}


func (f *Filter) Input(data []byte) {
	f.data <- data
}

func (f *Filter) Run(num int) {
	for {
		select {
		case msg := <- f.data:

			o := &oplogManagerPB.Oplog{}
			err := proto.Unmarshal(msg[4:], o)
			if err != nil {
				log.Printf("proto decode fail:%v",err)
				continue
			}

			for index,_ := range f.senderList {
				tt := f.senderList[index].Type()
				if tt == "inf" {
					data := f.model.ConverToInfP(o)
					f.senderList[index].Add(data)
				} else {
					ret := f.model.Filter(o, f.senderList[index].Type()  )
					if len(ret) == 0 {
						continue
					}
					f.senderList[index].Convert(ret)
				}
			}
			//f.input.Add(1)
		case <- f.ctx.Done():
			log.Printf("close filter worker:%v",num)
			return
		}
	}
}

func (f *Filter) Close() {
	f.cancel()
}
