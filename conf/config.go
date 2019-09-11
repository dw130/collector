package conf

import (
	"log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	Input  InputConf
	Filter FilterConf
	Output []OutPutConf
}

func ParseConf(path string) *Config {

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("ReadFile fail:%v",err)
		return nil
	}

	c := &Config{}

	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Printf("ReadFile fail:%v",err)
		return nil
	}
	return c
}