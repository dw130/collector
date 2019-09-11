package tool

import (
	"fmt"
	"time"
	"errors"
	"context"
	"encoding/json"
	"github.com/collector/stat"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	mongo * mongo.Client
	co    * mongo.Collection
	db    string
	collection string
	str  string
	stat   *stat.Static
	thr int64
}

func NewMongo(str string,db string,collection string,thr int64) *Mongo {
	ss := fmt.Sprintf("mongodb://%v/?readPreference=secondaryPreferred&SouthDB=admin",str)

	client, err := mongo.NewClient( options.Client().ApplyURI(ss) )
	if err != nil { 
		fmt.Printf("NewClient 1 err:%v\n",err)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		fmt.Printf("NewClient 2 err:%v\n",err)
		return nil 
	}

    m := Mongo{}
    m.mongo = client
    m.db = db
    m.collection = collection
    m.str = str
    m.stat = stat.NewStatic( fmt.Sprintf("mongo_table:%v_%v",db,collection)  )
    m.thr = thr

    dbC := client.Database(db)
    m.co = dbC.Collection(collection)
    return &m
}

func (m *Mongo) Get(key string) (map[string]interface{},error) {

	if m.stat.CurrentCount() > m.thr {
		return map[string]interface{}{},errors.New("get mongo too freq")
	}
	return map[string]interface{}{},errors.New("")
	
	object,_ := primitive.ObjectIDFromHex(key)
	ret := m.co.FindOne(context.Background(), bson.D{{"_id", object} })
	if ret == nil {
	    return map[string]interface{}{},errors.New("can not fetch data from db") 	
	}

	ss,_ := ret.DecodeBytes()
	el,err := ss.Elements()
	if err != nil {
	    return map[string]interface{}{},errors.New("can not fetch data from db") 	
	}

	p := map[string]interface{}{}
	for k,_ := range el {
		key := el[k].Key()
		val := el[k].Value()
		if key == "goods" {
			vv := val.String()
			pp := [] interface{} {}
			err := json.Unmarshal([]byte(vv), &pp)
			if err == nil {
				p["goods"] = pp
			}
		}
		switch val.Type {
			case bsontype.Double:
				p[key] = val.Double()
			case bsontype.String:
				p[key] = val.StringValue()
			case bsontype.Int32:
				p[key] = val.Int32()
			case bsontype.Int64:
				p[key] = val.Int64()
			case bsontype.ObjectID:
				p[key] = map[string]interface{} {"$oid": val.ObjectID().Hex()  }
			//case bsontype.EmbeddedDocument:
			//	fmt.Printf("*****catch it*****%v*****%v\n",val.String(), el[k].String())
		}
	}
	m.stat.Add(1)
	return p,nil
}

