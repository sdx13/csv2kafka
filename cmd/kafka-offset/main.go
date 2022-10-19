package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	defaultBrokerList = "localhost:9092"
	defaultTopic      = "hits"
)

func parseCmdArgs(ts int) int64 {
	if ts == -2 {
		return sarama.OffsetOldest
	}
	return sarama.OffsetNewest
}

func partitions(client sarama.Client, topic string) []int32 {
	parts, err := client.Partitions(topic)
	if err != nil {
		log.Println(err)
	}
	return parts
}

func printOffsets(parts []int32, client sarama.Client, topic string, ts int) {
	for _, partition := range parts {
		offset, err := client.GetOffset(topic, partition, parseCmdArgs(ts))
		if err != nil {
			log.Println(err)
		} else {
			fmt.Printf("%v:%v:%v\n", topic, partition, offset)
		}
	}
}

func main() {
	var broker string
	var topic string
	var ts int
	var interval int
	var count int
	flag.StringVar(&broker, "broker-list", defaultBrokerList, "<broker ip>:<port> (separated by commas)")
	flag.StringVar(&topic, "topic", defaultTopic, "Kafka topic")
	flag.IntVar(&ts, "time", -1, "timestamp")
	flag.IntVar(&interval, "i", 10, "interval")
	flag.IntVar(&count, "c", -1, "count")
	flag.Parse()

	client, err := sarama.NewClient(strings.Split(broker, ","), nil)
	if err != nil {
		panic(err)
	}
	parts := partitions(client, topic)

	for i := 0; count < 0 || i <= count; {
		printOffsets(parts, client, topic, ts)
		if count >= 0 {
			i++
		}
		if count < 0 || i <= count {
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
	client.Close()
}
