package main

type Record interface {
	unmarshalFromCSV(record []string)
	toStringMap() map[string]interface{}
	getSchema() string
}
