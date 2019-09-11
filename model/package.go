package model

import (
	"sync"
	"time"
	"fmt"
	"log"
	"bytes"
	"regexp"
	"strconv"
	"encoding/json"
	"github.com/collector/stat"
	"github.com/collector/conf"
	"github.com/garyburd/redigo/redis"
	"github.com/collector/model/tool"
	"github.com/collector/model/oplogManagerPB"
	client "github.com/influxdata/influxdb/client/v2"

)

type Package struct {
	packageSta  *stat.Static
	name     string
	conf     *conf.FilterConf
	redisIns   *redis.Pool
	mongoIns   map[string]*tool.Mongo
	ttl        int64
	dd  map[float64]int
	ss  sync.Mutex
}

var (
	FLAG = "53df5710b4c4d6383ae8e9a6"
	REP = regexp.MustCompile(`status=[^\s]*?`)
)

func init() {
	p := &Package{name:"package"}
	Reg("order_package",p)
}


func (pp *Package) Init( conf *conf.FilterConf,redisIns  *redis.Pool ) {
	pp.conf = conf
	pp.packageSta = stat.NewStatic("model:order_package")
	pp.redisIns = redisIns
	pp.mongoIns = map[string] *tool.Mongo{}
	pp.mongoIns["order.order_package"] = tool.NewMongo( conf.Mongo, "order","order_package", 600 )
	pp.mongoIns["order.cancel_order"] = tool.NewMongo( conf.Mongo, "order","cancel_order", 600 )
	pp.mongoIns["order.order"] = tool.NewMongo( conf.Mongo, "order","order", 600 )
	pp.ttl = conf.RedisTTl
	pp.dd = map[float64]int{}
}


func (pp *Package) SetData(table string,p map[string]interface{}, id string,tags map[string]string, fields map[string]interface{}, zbuf *bytes.Buffer ) {

	_type,ok := p["type"]

	if ok == false {
		_type = "gg"
	}
	seller := p["seller"]
	if seller != FLAG {
		seller = "others"
	} else {
		seller = "自营"
	}

	ssInter,ok := p["status"]
	status := 0.0
	if ok == true {
		switch sss := ssInter.(type) {
			case int32:
				status = float64(sss)
			case float64:
				status = sss
		}
	}

	var statusS string
	if status == 1.0 {
		statusS = "order"
	} else if status == 2.0 {
		statusS = "pay"
	} else {
		statusS = fmt.Sprintf("status%v",status)
	}

	tags["Stype"] = _type.(string)
	tags["seller"] = seller.(string)
	tags["status"] = statusS

	var package_id string
	package_idInter,ok := p["package_id"]
	if ok == false {
		//log.Printf("package_id fail:",err,string(val))
		package_id = "dd"
	} else {
		package_id = package_idInter.(string)
	}

	goodsInter,ok := p["goods"]
	goods := []interface{}{}

	if ok == false {
	} else {
		goods = goodsInter.([]interface{})
	}

	totalPrice := 0.0
	goodNum := 0
	for k,_ := range goods {
		var price float64
		var total float64
		singleGood := goods[k].(map[string]interface{})
		priceInter,_ := singleGood["price"]
		switch pp := priceInter.(type) {
			case float64:
				price = pp
			case map[string]interface {}:
				priceInt,ok := pp["$numberInt"]
				if ok == true {
					priceI,_ := strconv.Atoi(priceInt.(string))
					price = float64(priceI)
				}
				priceDouble,ok := pp["$numberDouble"]
				if ok == true {
					price,_ = strconv.ParseFloat(priceDouble.(string), 64) 
				}
		}

		totalInter,_ := singleGood["total"]
		switch pp := totalInter.(type) {
			case float64:
				total = pp
			case map[string]interface {}:
				totalInt,ok := pp["$numberInt"]
				if ok == true {
					totalI,_ := strconv.Atoi(totalInt.(string))
					total = float64(totalI)
				}
				totalIntDouble,ok := pp["$numberDouble"]
				if ok == true {
					total,_ = strconv.ParseFloat(totalIntDouble.(string), 64) 
				}
		}
		totalPrice = totalPrice + float64(total) * price
		goodNum = goodNum + 1
		if len(goods) == 0 {
			fmt.Printf("****oh no data****%+v\n",goods)
		}
	}

	fields["_id"] = id
	fields["totalPrice"] = totalPrice
	fields["goodNum"] = goodNum
	fields["package_id"] = package_id

	tt := map[string]interface{}{}
	tt["tags"] = tags
	tt["fields"] = fields
	jsonStu,_ := json.Marshal(tt)
	zbuf.Write(jsonStu)	

}

func (pp *Package) Filter(val *oplogManagerPB.Oplog, outType string) ([]byte) {
	return []byte{}
}

func (pp *Package) ConverToInfP(val *oplogManagerPB.Oplog) *client.Point {

	p := map[string]interface{} {}

	if val.Ns != "order.order_package" {
		return nil
	}

	data := val.Doc
	table := val.Ns

	tags := map[string]string{}
	fields := map[string]interface{} {}

	err := json.Unmarshal([]byte(data), &p)
	if err != nil {
		log.Printf("Unmarshal fail:%v,val:%v",err,val)
		return nil
	}

	idInter,_ := p["_id"]
	idIn,_ := idInter.(map[string]interface{})["$oid"]
	id := idIn.(string)

	tmp,ok := p["$set"]
	if ok == true {
		p = tmp.(map[string]interface{})
	}

	zbuf := bytes.NewBuffer(nil)

	var created float64

	if val.Op == 1 {
		pp.SetData(table,p,id,tags,fields,zbuf)
		rc := pp.redisIns.Get()
		defer rc.Close()
		key := fmt.Sprintf("%v_%v",table,id)
		rc.Do("SET", key, string( zbuf.Bytes() )  )
		rc.Do("EXPIRE", key, pp.ttl)

		createdAtInter,ok := p["created_at"]
		if ok == false {
		} else {
			created,_ = createdAtInter.(map[string]interface{})["$date"].(float64)
			created = created - 28800000
		}
		pp.packageSta.Add(1)
	}

	currentStatus,ok := p["status"]
	if val.Op == 2 && ok {
		pay_timeInter,okP := p["pay_time"]
		if okP == false {
			createdAtInter,okC := p["updated_at"]
			if okC == false {
			} else {
				created,_ = createdAtInter.(map[string]interface{})["$date"].(float64)
			}			
		} else {
			created = pay_timeInter.(map[string]interface{})["$date"].(float64)
		}
		created = created  - 28800000

		rc := pp.redisIns.Get()
		defer rc.Close()
		initStr, err := redis.String(rc.Do("Get", fmt.Sprintf("%v_%v",val.Ns,id)) )
		if err == nil {

			var ret map[string]interface{}
			err:=json.Unmarshal([]byte(initStr),&ret)
			if err != nil {
				fmt.Printf("catch Unmarshal err:%v\n",err)
				return nil
			}
			tmpTags,_ := ret["tags"]
			for k,v := range tmpTags.(map[string]interface{}) {
				tags[k] = v.(string)
			}
			fields = ret["fields"].(map[string]interface{})

			if currentStatus.(float64) == 2 {
				tags["status"] = "pay"
			} else {
				tags["status"] = fmt.Sprintf("status=%v",currentStatus)				
			}

		} else {
			ccc,_ := pp.mongoIns[table]
			ret,err := ccc.Get(id)
			if err != nil {
				log.Printf("fetch data from mongo fail:%v:%v",id,err)

				pp.SetData(table,p,id,tags,fields,zbuf)
			} else {
				//log.Printf("***fetch data from mongoIns***%+v\n",ret)
				pp.SetData(table,ret,id,tags,fields,zbuf)
			}
		}
	}

	//base_format := "2006-01-02 15:04:05"
	//dt := time.Unix( int64(created)／000, 0)

	//tt := time.Parse( base_format, fmt.Sprintf("%v-%v-%v %v:%v:00", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute()  ))
	f,ok := fields["goodNum"]
	if ok {
		switch ff := f.(type) {
			case float64:
				fields["goodNum"] = int64(ff)
			case int64:
				fields["goodNum"] = int64(ff)
				
		}
	}
	pt,_ := client.NewPoint(table, tags, fields, time.Now())

	//fmt.Printf("****check***%v***%v***%v\n",val.Op,tags,fields)

	return pt
}


