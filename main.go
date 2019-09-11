package main

import (
	"flag"
	"os"
	"os/signal"
	"github.com/collector/receiver"
	"github.com/collector/sender"
	"github.com/collector/filter"
	"github.com/collector/conf"
)



func main() {

    confPath := flag.String("conf","","path")

    flag.Parse()

    c := conf.ParseConf(*confPath)

    senderList := sender.FetchSenders(c)

    filterT := filter.NewFilter(c,senderList)

	kafka := receiver.NewKafkaIns(c,filterT)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case <-signals:
			//以后改成使用context关闭
			kafka.Close()
			filterT.Close()
			for k,_ := range senderList {
				senderList[k].Close()
			}
			return
		}
	}
}