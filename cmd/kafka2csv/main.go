package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro/v2"
	"gopkg.in/yaml.v2"
)

type config struct {
	MaxPollTimeout int `yaml:"max_poll_timeout,omitempty"`
	Count          int `yaml:"count,omitempty"`
	// MaxMessagesPerFile int    `yaml:"max_messages_per_file,omitempty"`
	OutputFormat    string `yaml:"output_format,omitempty"`
	AvroSchema      string `yaml:"avro_schema,omitempty"`
	KafkaTopic      string `yaml:"kafka_topic,omitempty"`
	KafkaProperties string `yaml:"kafka_properties,omitempty"`
}

func loadConfig(path string) (*config, error) {
	cfg := &config{}

	cfg.MaxPollTimeout = 20
	cfg.Count = 0
	// cfg.MaxMessagesPerFile = 10
	cfg.OutputFormat = "CSV"
	cfg.AvroSchema = "/home/osboxes/hits.avsc"
	cfg.KafkaTopic = "test"
	cfg.KafkaProperties = "/home/osboxes/consumer.properties"

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

type AvroCodec struct {
	codec *goavro.Codec
}

func NewAvroCodec(schemaFile string) (*AvroCodec, error) {
	schema, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		return nil, err
	}
	return &AvroCodec{codec: codec}, nil
}

func (c *AvroCodec) TextualFromBinary(binary []byte) []byte {
	// Test code
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

	return (textual)
}

// Load kafka consumer properties in a kafka config map
func loadKafkaConfig(filename string) (*kafka.ConfigMap, error) {
	consumerMap := kafka.ConfigMap{}
	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}

	defer func(file *os.File) {
		fileCloseErr := file.Close()
		if fileCloseErr != nil {
			panic(fileCloseErr)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		err := consumerMap.Set(line)
		if err != nil {
			return nil, err
		}
	}
	return &consumerMap, nil
}

type KafkaReader struct {
	topic  string
	reader *kafka.Consumer
}

func NewKafkaReader(cfg *config) (*KafkaReader, error) {
	consumerMap, err := loadKafkaConfig(cfg.KafkaProperties)
	if err != nil {
		log.Fatalln("Could not parse kafka config", err)
		return nil, err
	}

	c, err := kafka.NewConsumer(consumerMap)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{cfg.KafkaTopic}, nil)
	if err != nil {
		return nil, err
	}

	k := KafkaReader{
		topic:  cfg.KafkaTopic,
		reader: c,
	}
	return &k, nil
}

type CsvWriter struct {
	writer *csv.Writer
}

func NewCsvWriter() *CsvWriter {
	w := csv.NewWriter(os.Stdout)
	return &CsvWriter{writer: w}
}

func (w *CsvWriter) Write(r []string) error {
	if err := w.writer.Write(r); err != nil {
		return err
	}

	// Write any buffered data to the underlying writer (standard output).
	w.writer.Flush()

	if err := w.writer.Error(); err != nil {
		return err
	}
	return nil
}

// Extract values from the texfual form. Nullable field values get encoded as:
//   "name": {"type" : "value"}
func valuesFromTextual(inputMap map[string]interface{}) []string {
	var s []string
	for _, value := range inputMap {
		switch nestedMap := value.(type) {
		case map[string]interface{}:
			for _, val := range nestedMap {
				s = append(s, fmt.Sprintf("%s", val))
			}
		default:
			s = append(s, fmt.Sprintf("%s", value))
		}
	}
	return s
}

func decodeFields(textual []byte) map[string]interface{} {
	var fieldsMap map[string]interface{}
	decoder := json.NewDecoder(strings.NewReader(string(textual)))
	decoder.UseNumber()
	if err := decoder.Decode(&fieldsMap); err != nil {
		log.Fatal(err)
	}

	return fieldsMap
}

func consumeKafkaMessages(cfg *config, c *KafkaReader, codec *AvroCodec, w *CsvWriter) {
	var numRead = 0
	for {
		msg, err := c.reader.ReadMessage(time.Duration(cfg.MaxPollTimeout) * time.Second)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
		fieldsMap := decodeFields(codec.TextualFromBinary(msg.Value))
		record := valuesFromTextual(fieldsMap)
		err = w.Write(record)
		if err != nil {
			log.Printf("error writing record to csv: %v", err)
			break
		}
		numRead++
		if numRead >= cfg.Count {
			break
		}
	}
	c.reader.Close()
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "config/config.yml", "config file")
	flag.Parse()

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("config %v", err)
	}

	// Read schema file
	codec, err := NewAvroCodec(cfg.AvroSchema)
	if err != nil {
		log.Fatalln("Could not parse schema", err)
	}

	consumer, err := NewKafkaReader(cfg)
	if err != nil {
		log.Fatalln("Could not create Kafka consumer")
	}

	writer := NewCsvWriter()

	consumeKafkaMessages(cfg, consumer, codec, writer)
}
