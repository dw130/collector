package model

import (
	"fmt"
	"log"
	"bytes"
	"time"
	"strings"
	"regexp"
	"encoding/json"
	"github.com/collector/conf"
	"github.com/collector/stat"
	"github.com/garyburd/redigo/redis"
	"github.com/collector/model/oplogManagerPB"
	"github.com/collector/model/tool"
	client "github.com/influxdata/influxdb/client/v2"

)

type Common struct {
	stat   *stat.Static
	name   string
	conf   *conf.FilterConf
	tabList map[string] *tool.Mongo

	tagList map[string]string
	fieldList map[string]string
	timeFiled []string
	timeShift float64
	timeUnit  int
	timeFlag  []int
	redisIns   *redis.Pool
	ttl       int64
}

var (
	RE = regexp.MustCompile(`status=status_[\d]*`)
)

func NewCommon( conf *conf.FilterConf,redisIns  *redis.Pool ) *Common {

	c := &Common{}
	c.stat = stat.NewStatic("model:common")
	c.Init(conf,redisIns)
	c.tabList = map[string] *tool.Mongo{}

	c.tagList = map[string]string{}
	c.fieldList = map[string]string{}
	c.redisIns = redisIns
	c.ttl = conf.RedisTTl

	for k,_ := range conf.Table {
		tmp := strings.Split( conf.Table[k], "." )
		db := tmp[0]
		coll := tmp[1]
		c.tabList[ conf.Table[k]  ] = tool.NewMongo( conf.Mongo, db, coll,450 )
	}

	for k,_ := range conf.TagList {
		c.tagList[ k  ] = conf.TagList[k].(string)
	}

	for k,_ := range conf.FieldList {
		c.fieldList[ k  ] = conf.FieldList[k].(string)
	}

	c.timeFiled = conf.Time
	c.timeShift = conf.TimeShift
	c.timeUnit  = conf.TimeUnit
	c.timeFlag = []int{}

	for k,_ := range c.timeFiled {
		if c.timeFiled[k] == "created_at" || c.timeFiled[k] == "updated_at" || c.timeFiled[k] == "paid_at" {
			c.timeFlag = append(c.timeFlag,1)
		} else {
			c.timeFlag = append(c.timeFlag,0)
		}
	}

	log.Printf("common conf:%+v",c)
	return c
}

func (c *Common) Init(conf *conf.FilterConf,redisIns  *redis.Pool ) {
	c.conf = conf
}


func (c *Common) InsertData( table string,zbuf *bytes.Buffer,p map[string]interface{}, idStr string ) {

	zbuf.Write( []byte( table ) )

	for k,v := range c.tagList {
		data,ok := p[k]

		if ok == false {
			continue
		}
		switch d := data.(type) {
			case map[string]interface{}:
				vv := d[v].(string)
				vv = strings.Replace(vv," ","",-1)
				zbuf.Write( []byte(fmt.Sprintf(",%v=%v",k,vv)) )
			case string:
				d = strings.Replace(d," ","",-1)
				zbuf.Write( []byte(fmt.Sprintf(",%v=%v",k,d)) )
			case float64,int,int32,float32:
				ss := fmt.Sprintf("%v_%v",k,d)
				zbuf.Write( []byte(fmt.Sprintf(",%v=%v",k,ss)) )
			case bool:
				zbuf.Write( []byte(fmt.Sprintf(",%v=%v_%v",k,k,d)) )
			default:
				zbuf.Write( []byte(fmt.Sprintf(",%v=%v_%v",k,k,d)) )
		}
	}

	zbuf.Write( []byte(" ") )

	count := 0
	for k,v := range c.fieldList {
		data,ok := p[k]
		if ok == false {
			continue
		}
		switch d := data.(type) {
			case map[string]interface{}:
				vv := d[v]
				if count == 0 {
					zbuf.Write( []byte(fmt.Sprintf("%v=%v",k,vv)) )
				} else {
					zbuf.Write( []byte(fmt.Sprintf(",%v=%v",k,vv)) )
				}
			//case []interface{}:
			case string:
				if count == 0 {
					zbuf.Write( []byte(fmt.Sprintf(`%v="%v"`,k,d)) )
				} else {
					zbuf.Write( []byte(fmt.Sprintf(`,%v="%v"`,k,d)) )
				}
			default:
				if count == 0 {
					zbuf.Write( []byte(fmt.Sprintf("%v=%v",k,d)) )
				} else {
					zbuf.Write( []byte(fmt.Sprintf(",%v=%v",k,d)) )
				}
		}
		count = count + 1
	}

	if idStr != "" {
		if count == 0 {
			zbuf.Write( []byte(fmt.Sprintf(`idoid="%v"`,idStr)) )
		} else {
			zbuf.Write( []byte(fmt.Sprintf(`,idoid="%v"`,idStr)) )	
		}
	}
}

func (c *Common) Filter(val *oplogManagerPB.Oplog,input string) []byte {

	zbuf := bytes.NewBuffer(nil)

	table := val.Ns
	ccc,ok := c.tabList[table]
	if ok == false {
		return []byte{}
	}

	data := val.Doc

	p := map[string]interface{} {}

	err := json.Unmarshal([]byte(data), &p)
	if err != nil {
		log.Printf("Unmarshal fail:%v,val:%v",err,val)
		return []byte{}
	}

	idStr := ""
	_idInt,ok := p["_id"]
	if ok == true {
		switch dd := _idInt.(type) {
			case map[string]interface{}:
				t,ok := dd["$oid"]
				if ok == true {
					idStr = t.(string)
				}
		}
	}

	tmp,ok := p["$set"]
	if ok == true {
		p = tmp.(map[string]interface{})
	}

	created := -1.0
	for k,_ := range c.timeFiled {
		createdInter,ok := p[ c.timeFiled[k] ]
		if ok == false {
			//log.Printf("lost time:%v timeField:%v",p,c.timeFiled)
			continue
		}

		realTime := 0.0
		switch tt := createdInter.(type) {
			case map[string]interface{}:
				for k,_ := range tt {
					realTime = tt[k].(float64)
				}
				if c.timeFlag[k] ==  1 {
					realTime = realTime - 28800000
				}
			case float64:
				realTime = tt
		}

		//时间单位是秒，需要再* 1000
		if c.timeUnit == 1 && c.timeFlag[k] == 0 {
			created = realTime - c.timeShift
			created = created * 1000 
		} else {
			created = realTime
		}
		break
	}

	if created < 0 {
		created =  float64( time.Now().Unix() * 1000 )
	}

	if val.Op == 1 {
		c.InsertData(table,zbuf,p,idStr)
		rc := c.redisIns.Get()
		defer rc.Close()
		key := fmt.Sprintf("%v_%v",table,idStr)
		rc.Do("SET", key, string( zbuf.Bytes() )  )
		rc.Do("EXPIRE", key, c.ttl)

	}

	currentStatus,ok := p["status"]
	if val.Op == 2 && ok {

		rc := c.redisIns.Get()
		defer rc.Close()
		initStr, err := redis.String(rc.Do("Get", fmt.Sprintf("%v_%v",table,idStr)) )
		if err == nil {
			//to do,read date from inital mongo to ensure data not lose
			initStr = RE.ReplaceAllString(initStr, fmt.Sprintf("status=status_%v",currentStatus))
			//if table == "customer.membership_order" || table == "customer.renew_record" {
			//	fmt.Printf("*catch data***%v**%v\n",fmt.Sprintf("%v_%v",table,idStr), initStr)
			//}
			zbuf.Write( []byte( initStr )  )
			c.stat.Add(1)
		} else {
			ret,err := ccc.Get(idStr)
			if err != nil {
				log.Printf("fetch data from mongo fail:%v:%v",idStr,err)
				c.InsertData(table,zbuf,p,idStr)
			} else {
				c.InsertData(table,zbuf,ret,idStr)
				if table == "customer.membership_order" || table == "customer.renew_record" {
					fmt.Printf("*catch mongo***%v**%v\n",fmt.Sprintf("%v_%v",table,idStr), string( zbuf.Bytes()  ))
				}
			}
		}

		//if table == "customer.membership_order" || table == "customer.renew_record" {
		//	fmt.Printf("***get****%v****%v****%v***err:%v\n",fmt.Sprintf("%v_%v",table,idStr),ret,string(zbuf.Bytes()),err)
		//}
	}

	zbuf.Write( []byte( fmt.Sprintf(" %13.0f000000",created) ) )
	zbuf.Write( []byte("\n"))

	//if table == "customer.membership_order" || table == "customer.renew_record" {
	//	fmt.Printf("***final***%v\n", string( zbuf.Bytes()   ) )
	//}
	return zbuf.Bytes()
}


func (c *Common) ConverToInfP(val *oplogManagerPB.Oplog) *client.Point {
	return nil
}

