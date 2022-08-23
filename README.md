[![Go Report Card][]][1] [![License][]][2] [![Quality Gate Status][]][3]

# csv2kafka

Experiments on reading compressed CSV data and writing to Kafka in Avro
format.

## Platform

Tested to work on CentOS 7.

## Installation

```shell
git clone git@github.com:sdx13/csv2kafka.git
cd cmd/csv2kafka
go install
```

## Testing

Finding security vulnerabilities:

```shell
trivy fs --ignore-unfixed .
```
Finding code smells:

```shell
golangci-lint run  -c <config_file>
```

## Usage

TBD

[Go Report Card]: https://goreportcard.com/badge/github.com/sdx13/csv2kafka
[1]: https://goreportcard.com/report/github.com/sdx13/csv2kafka
[License]: https://img.shields.io/badge/license-MIT-green
[2]: https://github.com/sdx13/csv2kafka/blob/main/LICENSE
[Quality Gate Status]: https://sonarcloud.io/api/project_badges/measure?project=sdx13_csv2kafka&metric=alert_status
[3]: https://sonarcloud.io/summary/new_code?id=sdx13_csv2kafka
