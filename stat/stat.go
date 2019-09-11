package stat

import (
	"time"
	"log"
	"sync/atomic"
	client "github.com/influxdata/influxdb/client/v2"
)

var (
	conn client.Client
)

func init() {
	conn, _ = client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://:8000",
		Username: "",
		Password: "",
	})
}



type Static struct {
	count  int64
	_type string
	currentCount int64
}

func (s *Static) Add(num int) {
	atomic.AddInt64(&s.count, int64(num))
}


func NewStatic(_type string) *Static {
	s := &Static{}
	s.count = 0
	s._type = _type
	s.currentCount = 0
	go s.Run()
	return s
}


func (s *Static) CurrentCount() int64 {
	return s.currentCount
}

func (s *Static) send() {
	bp,_ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "ordermonitor",
	})
 
	tags := map[string]string{"name": s._type}
	fields := map[string]interface{}{ "value":  s.currentCount }
 
	pt,_ := client.NewPoint("monitor", tags, fields, time.Now())
	bp.AddPoint(pt)
 
	if err := conn.Write(bp); err != nil {
		log.Fatal(err)
	}
}

func (s *Static) Run() {

	startCollect := time.Tick(30 * time.Second)
	var begin int64
	begin = 0
	//loop:
	for {
		select {
			case <- startCollect:
				nn := atomic.LoadInt64(&s.count)
				del := nn - begin
				log.Printf("num check:%v %v\n",s._type, del)
				begin = nn
				s.currentCount = del
				s.send()
				
				//zbuf.Write( []byte( fmt.Sprintf("monitor,name=%v value=%v %v\n",s._type,del,time.Now().UnixNano())
				//inf.Convert(  zbuf.Bytes() )	
		}
	}
}