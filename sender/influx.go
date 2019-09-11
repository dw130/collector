package sender

import (
	"net/http"
	"net"
	"time"
	"crypto/tls"
	"fmt"
	"bytes"
	"compress/gzip"
	"net/url"
	"io/ioutil"
	"math/rand"
	//"strings"
	"log"
	//"github.com/collector/model"
	"github.com/collector/stat"
	"github.com/collector/conf"
	client "github.com/influxdata/influxdb/client/v2"
)

var (
	_ = fmt.Sprintf("")
)

var (
	httpClient = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     2 * time.Second,
		DisableCompression:  false,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		Dial: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 2 * time.Second,
	}}
)

type Influx struct {
	Addr string 
	httpClient *http.Client
	httpAddr string
	queryAddr string
	pingAddr string
	flushInterval time.Duration
	maxSize int

	worker  int
	worker_stop map[int] []chan bool
	flag bool
	queue1 chan *Data
	input     *stat.Static

	senderType    string
	db      string
	rp      string
}

type Data struct {
	Name string
	Dd   []byte
	Leng int
}

func (i *Influx) Add(p *client.Point) {
	
}

func (i *Influx) Convert(ret []byte) {
	i.PutPoints(ret,1)
}

func (i *Influx) Init( cc conf.OutPutConf ){

	add := cc.Addr
	worker := cc.Worker

	i.queue1 = make(chan *Data,1000000)

	i.Addr = add
	i.httpClient = httpClient
	i.httpAddr = fmt.Sprintf("http://%s/write", i.Addr)

	i.worker = worker
	i.worker_stop = map[int] []chan bool{}
	i.flag = false
	i.flushInterval = 3 * time.Second
	i.maxSize = 2000
	i.input = stat.NewStatic( fmt.Sprintf("influxdb_%v",cc.Name))

	i.senderType = "influxdb"
	i.db = cc.Dbname
	i.rp = cc.Rp

	for k := 0; k < i.worker; k ++ {
		i.worker_stop[k] =  []chan bool{}
		i.worker_stop[k] = append(i.worker_stop[k], make(chan bool))
		go i.RunSer( k,i.worker_stop[k][0] )
	}

	log.Printf("run influxdb Ser:%v",i.Addr," worker:%v",i.worker)
}

func init() {
	Reg("influxdb",&Influx{})
}


func (i *Influx) Type() string {
	return i.senderType
}

func (i *Influx) Close() {
	for k,_ := range i.worker_stop {
		i.worker_stop[k][0] <- true
	}

	log.Printf("close influxdb:", i.Addr)
}

func ( i *Influx) writePointsBytes(ret []byte ) {

	u, err := url.Parse(i.httpAddr)
	
	if err != nil {
		log.Printf("failed to parse influxdb instance addr: %v", err)
		return
	}
	p := url.Values{}
    
	p.Add("db", i.db)
	p.Add("rp", i.rp)
	//p.Add("precision", precision)
	u.RawQuery = p.Encode()

	zbuf := bytes.NewBuffer(nil)
	gz := gzip.NewWriter(zbuf)
	gz.Write( ret )
	gz.Flush()
	gz.Close()

	req, err := http.NewRequest("POST", u.String(), bytes.NewBuffer( zbuf.Bytes() ) )
	if err != nil {
		fmt.Printf("NewRequest Error: %v", err)
		return
	}

	// all data should be gzip encoded.
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := i.httpClient.Do(req)
	if err != nil {
		fmt.Printf("influx.PostPoints - post data failed: %v", err)
		//errCh <- err
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		
		if _, err := ioutil.ReadAll(resp.Body); err != nil {
			fmt.Printf("status code not 2xx",err)
			return
		} 
	}

	//fmt.Printf("*******ret*******%v***%v\n",string(ret),u.RawQuery)
}


func (i *Influx) PutPoints(b []byte,c int) {

	a := Data{  Dd: b,Leng:c , Name: ""  }

	i.queue1 <- &a
}



func (i *Influx) RunSer(count int,stop chan bool) {

	total := map[string] *bytes.Buffer{}

	num := rand.Int31n(999)
	time.Sleep(time.Duration(num) * time.Millisecond)

	startCollect := time.Tick(i.flushInterval)

	tc := map[string] int{}

	loop:
	for {
		select {

			case <- startCollect:
				i.input.Add(len(total))
				for k,_ := range total {

					i.writePointsBytes(total[k].Bytes())
					total[ k  ].Reset()
					delete( total,k  )
				}

			case <- stop:
				break loop

			case ret := <- i.queue1:
				if ret.Leng >= i.maxSize {
					i.writePointsBytes( ret.Dd)
					continue
				}

				_,ok := total[ret.Name]					
				if ok == false {
					total[ret.Name] = bytes.NewBuffer(nil)
					tc[ ret.Name ] = 0
				}

				tc[ ret.Name ] = tc[ ret.Name ] + ret.Leng
				total[ret.Name].Write( ret.Dd )

				//长度太长 直接发出去
				if tc[ ret.Name ] > i.maxSize {
					i.input.Add(i.maxSize)
					//fmt.Printf("*******3******\n")
					i.writePointsBytes(total[ ret.Name  ].Bytes() )
					total[ ret.Name  ].Reset()
					delete( total, ret.Name )
				}
		}
	}
	log.Printf("close RunSer",i.Addr," worker no:",count)
}



