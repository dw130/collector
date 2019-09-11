package model

import (
	"fmt"
	"github.com/collector/model/oplogManagerPB"
	"github.com/garyburd/redigo/redis"
	"github.com/collector/conf"
	client "github.com/influxdata/influxdb/client/v2"
)


type General interface {
	Filter(*oplogManagerPB.Oplog, string) []byte 
	Init(*conf.FilterConf,*redis.Pool)
	ConverToInfP(*oplogManagerPB.Oplog) *client.Point
}


var (
	_ = fmt.Sprintf("")
	Mapping = map[string] General{}
)


func Reg(key string,s General) {
	Mapping[key] = s
}

func FetchInstance(conf *conf.FilterConf, re *redis.Pool) General {

	key := conf.Plugin

	if key == "" {
		return NewCommon(conf,re)
	}

	s,ok := Mapping[ key ]
	if ok == false {
		return nil
	}
	s.Init(conf,re)
	return s
}
