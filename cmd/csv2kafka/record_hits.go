package main

import (
	"log"
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
	if r.startTime != "" {
		v, err := r.getStartTime()
		if err != nil {
			// XXX/PDP Log an error message
			v = 0
		}
		datum["start_time"] = goavro.Union("long", v)
	} else {
		datum["start_time"] = goavro.Union("null", nil)
	}

	if r.endTime != "" {
		v, err := r.getEndTime()
		if err != nil {
			v = 0
		}
		datum["end_time"] = goavro.Union("long", v)
	} else {
		datum["end_time"] = goavro.Union("null", nil)
	}

	if r.mobile != "" {
		v, err := strconv.ParseInt(r.mobile, 10, 64)
		if err != nil {
			v = 0
		}
		datum["mobile_phone"] = goavro.Union("long", v)
	} else {
		datum["mobile_phone"] = goavro.Union("null", nil)
	}

	return datum
}

func (r *hitsRecord) getStartTime() (int64, error) {
	//t, err := time.Parse("2006-01-02T15:04:05.999Z07:00", in)
	t, err := time.Parse("01/02/06-15:04:05", r.startTime)
	if err != nil {
		log.Printf("Error parsing %v\n", r.startTime, err)
		return 0, err
	}

	return t.Unix(), nil
}

func (r *hitsRecord) getEndTime() (int64, error) {
	t, err := time.Parse("01/02/06-15:04:05", r.endTime)
	if err != nil {
		log.Printf("Error parsing %v\n", r.endTime, err)
		return 0, err
	}

	return t.Unix(), nil
}
