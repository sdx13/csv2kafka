[![Go Report Card][]][1] [![License][]][2]

# csv2kafka

Experiments on reading compressed CSV data and writing to Kafka in Avro
format.

## Platform

Tested to work on CentOS 7.

## Installation

```sh
git clone git@github.com:sdx13/csv2kafka.git
cd cmd/csv2kafka
go install
```

## Usage

```sh
sudo yum install docker-compose
```

## TODO

- Prevent non-fast-forward merges to master.
- Improve linter config and resolve issues
- Write program to list Kafka topics
- Write program to consumer Avro messages from Kafka topic

[Go Report Card]: https://goreportcard.com/badge/github.com/sdx13/csv2kafka
[1]: https://goreportcard.com/report/github.com/sdx13/csv2kafka
[License]: https://img.shields.io/badge/license-MIT-green
[2]: https://github.com/sdx13/csv2kafka/blob/main/LICENSE
