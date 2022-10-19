# kafka-offset

The `kafka-offset` program provides functionality similar to [GetOffsetShell](https://cwiki.apache.org/confluence/display/KAFKA/System+Tools). It can be used to monitor if records are being continuously written to a Kafka topic.

## Installation

As of now, the executable has to be built from sources:

```shell
$ git clone git@github.com:sdx13/csv2kafka.git
$ cd csv2kafka/cmd/kafka-offset
$ go build
```

Transfer the executable to a location in the `PATH` environment variable.

```shell
$ sudo cp kafka-offset /usr/local/bin/
```

## Usage

Example:

```shell
$ kafka-offset --broker-list localhost:9092 --topic hits --time -1
```

Sample output:

```
hits:0:2
```

Example for [GetOffsetShell](https://cwiki.apache.org/confluence/display/KAFKA/System+Tools) is very similar:

```shell
$ /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic hits --time -1
```

## Performance

Usually much times faster than GetOffsetShell.
