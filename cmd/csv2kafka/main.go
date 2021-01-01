package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/linkedin/goavro/v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/yaml.v2"
)

const (
	hitsSchema = `{
        "type" : "record",
        "name" : "hits",
        "fields" : [
			{"name": "start_time", "type" : ["null", "long"]}
		]
	}`
)

type config struct {
	KafkaBrokers string `yaml:"kafka_brokers,omitempty"`
	KafkaTopic   string `yaml:"kafka_topic,omitempty"`
}

func loadConfig(path string) (*config, error) {
	cfg := &config{}

	cfg.KafkaBrokers = "127.0.0.1:9092"
	cfg.KafkaTopic = "hits"

	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(content, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// Abstract Entities
//
// We are dealing with at least two abstractions: data source and encoding.
// Data sources are files on local/SFTP filesystem or a network port. Encoding
// is about the data being in plaintext or gzip compressed.
//

// things like SFTP/local FS, network port (future)
type RecordSource struct {
}

// something that lets read a regular or gzip compressed file
type RecordReader struct {
}

// reads uncompressed files
type FileReader struct {
	s *bufio.Scanner
}

type hitsRecord struct {
	startTime string
}

// XXX/PDP Perhaps it should return error as well
func (r *hitsRecord) unmarshalFromCSV(record []string) {
	if len(record) > 1 {
		r.startTime = record[1]
	}
}

func (r *hitsRecord) toStringMap() map[string]interface{} {
	datum := map[string]interface{}{
		"start_time": -1,
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
	return datum
}

func (r *hitsRecord) getStartTime() (int64, error) {
	//t, err := time.Parse("2006-01-02T15:04:05.999Z07:00", in)
	t, err := time.Parse("01/02/06-15:04:05", r.startTime)
	if err != nil {
		log.Printf("Error parsing %v %v\n", r.startTime, err)
		return 0, err
	}

	return t.Unix(), nil
}

func NewFileReader(filePath string) (*FileReader, error) {
	r := &FileReader{}
	f, err := os.Open(filePath)
	if err == nil {
		s := bufio.NewScanner(f)
		if err == nil {
			r.s = s
		}
	}
	return r, err
}

func (r *FileReader) Read() (string, error) {
	if r.s.Scan() {
		return string(r.s.Text()), nil
	}
	return "", errors.New("failed to scan")
}

// reads gzip compressed files
type GzipReader struct {
	s *csv.Reader
}

func NewGzipReader(filePath string) (*GzipReader, error) {
	r := &GzipReader{}
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	g, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	s := csv.NewReader(g)
	r.s = s
	return r, err
}

func (r *GzipReader) Read() ([]string, error) {
	return r.s.Read()
	/*
		if r.s.Scan() {
			return string(r.s.Text()), nil
		}
		return "", errors.New("failed to scan")
	*/
}

type KafkaWriter struct {
	topic    string
	writer   *kafka.Producer
	delivery chan kafka.Event
}

func NewKafkaWriter(cfg *config) (*KafkaWriter, error) {
	var brokers = cfg.KafkaBrokers

	w, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		return nil, err
	}
	k := KafkaWriter{
		topic:    cfg.KafkaTopic,
		writer:   w,
		delivery: make(chan kafka.Event),
	}
	return &k, nil
}

func (w *KafkaWriter) Write(p []byte) (int, error) {
	var topic = w.topic
	_ = w.writer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          p,
	}, w.delivery)
	e := <-w.delivery
	m := e.(*kafka.Message)
	return 0, m.TopicPartition.Error
}

type AvroCodec struct {
	codec *goavro.Codec
}

func NewAvroCodec(schema string) (*AvroCodec, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}
	return &AvroCodec{codec: codec}, nil
}

// Convert string JSON data to Avro encoded byte array
func (c *AvroCodec) BinaryFromJson(text string) ([]byte, error) {
	native, _, err := c.codec.NativeFromTextual([]byte(text))
	if err != nil {
		return nil, err
	}

	binary, err := c.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	return binary, nil
}

func (c *AvroCodec) BinaryFromNative(native interface{}) ([]byte, error) {
	binary, err := c.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}
	return binary, nil
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "config.yml", "config file")
	flag.Parse()
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("config %v", err)
	}

	var schema = hitsSchema
	codec, err := NewAvroCodec(schema)
	if err != nil {
		log.Fatalln("Could not parse schema", err)
	}

	writer, err := NewKafkaWriter(cfg)
	if err != nil {
		log.Fatal("Could not create Kafka writer")
	}
	fileName := "hits.csv.gz"
	//r, err := NewFileReader(fileName)
	r, err := NewGzipReader(fileName)

	// XXX/PDP Rigidity! The record and rest of the code are at different
	// levels of abstraction making it difficult to process something of a
	// different type.
	data2 := &hitsRecord{}
	if err != nil {
		log.Fatal("Could not open input file")
	}
	for {
		record, err := r.Read()
		if err != nil {
			break
		}
		data2.unmarshalFromCSV(record)
		binary, err := codec.BinaryFromNative(data2.toStringMap())
		if err != nil {
			log.Fatalln("Could not convert to binary", err)
		}
		_, err = writer.Write(binary)
		if err != nil {
			log.Println("Error when writing to Kafka", err)

		}
	}
	// Web: https://github.com/linkedin/goavro/issues/121
}
