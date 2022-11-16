[![Go Report Card][]][1] [![License][]][2] [![Quality Gate Status][]][3]

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

- Improve linter config and resolve issues
- Write program to list Kafka topics
- Write program to consumer Avro messages from Kafka topic
- Prevent non-fast-forward merges to master.

## Completed Items

- Explore code analysis via [sonarcloud](https://sonarcloud.io/)

[Go Report Card]: https://goreportcard.com/badge/github.com/sdx13/csv2kafka
[1]: https://goreportcard.com/report/github.com/sdx13/csv2kafka
[License]: https://img.shields.io/badge/license-MIT-green
[2]: https://github.com/sdx13/csv2kafka/blob/main/LICENSE
[Quality Gate Status]: https://sonarcloud.io/api/project_badges/measure?project=sdx13_csv2kafka&metric=alert_status
[3]: https://sonarcloud.io/summary/new_code?id=sdx13_csv2kafka
