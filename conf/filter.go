package conf

type FilterConf struct {
	Plugin     string
	Table   []string
	Worker  int
	Redis   string
	TagList   map[string]interface{} `yaml:"taglist,omitempty"`
	FieldList map[string]interface{}
	Time    []string
	TimeShift float64
	TimeUnit int
	RedisHost string
	RedisPass string
	RedisTTl  int64
	Mongo     string
}