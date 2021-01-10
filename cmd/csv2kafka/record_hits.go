package main

import (
	"log"
	"net"
	"strconv"
	"time"

	"github.com/linkedin/goavro/v2"
)

type hitsRecord struct {
	startTime string
	endTime   string
	mobile    string
}

func (r *hitsRecord) getSchema() string {
	return `{
        "type" : "record",
        "name" : "hits",
        "fields" : [
	    {"name": "start_time", "type" : ["null", "long"]},
       	    {"name": "end_time", "type" : ["null", "long"]},
	    {"name": "mobile_phone", "type" : ["null", "long"]}
		]
	}`
}

// XXX/PDP Perhaps it should return error as well
func (r *hitsRecord) unmarshalFromCSV(record []string) {
	if len(record) > 2 {
		r.startTime = record[0]
		r.endTime = record[1]
		r.mobile = record[2]
	}
}

func (r *hitsRecord) toStringMap() map[string]interface{} {
	datum := map[string]interface{}{
		"start_time":   -1,
		"end_time":     -1,
		"mobile_phone": -1,
	}
	table := [...]struct {
		s        string
		avroType string
		nullable bool
		fn       func(string) interface{}
		name     string
	}{
		{r.startTime, "long", true, getTime, "start_time"},
		{r.endTime, "long", true, getTime, "end_time"},
		{r.mobile, "long", true, getLong, "mobile_phone"},
	}

	for _, row := range table {
		str := row.s
		if str == "" {
			datum[row.name] = nil
		} else {
			// XXX/PDP Transformation functions don't return
			// error. Somethine more elaborate is required if we
			// want varied behaviour in case of error.
			v := row.fn(str)
			if row.nullable {
				datum[row.name] = goavro.Union(row.avroType, v)
			} else {
				datum[row.name] = v
			}
		}
	}

	return datum
}

func getIP(s string) interface{} {
	ip := net.ParseIP(s)
	if ip == nil {
		log.Printf("Error parsing IP %v\n", s)
		ip = []byte{0x00, 0x00, 0x00, 0x00}
	}
	return []byte(ip)
}

func getTime(s string) interface{} {
	t, err := time.Parse("01/02/06-15:04:05", s)
	if err != nil {
		log.Printf("Error parsing %v: %v\n", s, err)
		return 0
	}
	return t.Unix()
}

func getLong(s string) interface{} {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("Error parsing long %v: %v\n", s, err)
		v = 0
	}
	return v
}

func getInt(s string) interface{} {

	v, err := strconv.Atoi(s)
	if err != nil {
		log.Printf("Error parsing int %v: %v\n", s, err)
		v = 0
	}
	return v
}

func getString(s string) interface{} {
	return s
}
