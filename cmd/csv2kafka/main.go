package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/linkedin/goavro/v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"gopkg.in/yaml.v2"
)

type config struct {
	KafkaBrokers string `yaml:"kafka_brokers,omitempty"`
	KafkaTopic   string `yaml:"kafka_topic,omitempty"`
	InputDir     string `yaml:"input_dir,omitempty"`
	ReadyDir     string `yaml:"ready_dir,omitempty"`
	WaitInterval int    `yaml:"wait_interval,omitempty"`
}

func loadConfig(path string) (*config, error) {
	cfg := &config{}

	cfg.KafkaBrokers = "127.0.0.1:9092"
	cfg.KafkaTopic = "hits"
	cfg.InputDir = "input"
	cfg.ReadyDir = "ready"
	cfg.WaitInterval = 30

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
// Data sources are files on local/SFTP filesystem or a network port.
// Encoding is about the data being in plaintext or gzip compressed.
//

// things like SFTP/local FS, network port (future)
type RecordSource interface {
	Read() (string, error)
}

// something that lets read a regular or gzip compressed file
type RecordReader struct {
}

// reads uncompressed files
type FileReader struct {
	s *bufio.Scanner
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

func (c *AvroCodec) TextualFromBinary(binary []byte) {
	//Test code
	// Convert binary Avro data back to native Go form
	native, _, err := c.codec.NativeFromBinary(binary)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to textual Avro data
	textual, err := c.codec.TextualFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(textual))

}

func recordFactory() Record {
	return &hitsRecord{}
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "config.yml", "config file")
	flag.Parse()
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("config %v", err)
	}

	var data2 = recordFactory()
	var schema = data2.getSchema()
	codec, err := NewAvroCodec(schema)
	if err != nil {
		log.Fatalln("Could not parse schema", err)
	}

	writer, err := NewKafkaWriter(cfg)
	if err != nil {
		log.Fatal("Could not create Kafka writer")
	}
	recordReader, err := NewFilesystemReader(cfg)
	if err != nil {
		log.Fatal("Could not open dir for reading")
	}
	for {
		record, err := recordReader.Read()
		if err != nil {
			break
		}
		data2.unmarshalFromCSV(record)
		binary, err := codec.BinaryFromNative(data2.toStringMap())
		if err != nil {
			// XXX/PDP Audit this error message. It usually
			// denotes receiving a record that does not have a
			// mandatory field.
			log.Println("Could not convert to binary", err)
			continue
		}

		//codec.TextualFromBinary(binary)
		_, err = writer.Write(binary)
		if err != nil {
			log.Println("Error when writing to Kafka", err)

		}
	}
	// Web: https://github.com/linkedin/goavro/issues/121
}
