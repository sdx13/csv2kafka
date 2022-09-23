# kafka2csv

The `kafka2csv` program writes to stdout Avro encoded data from a Kafka topic.
It is meant to assist in testing `csv2kafka` by being the latter's inverse.

## TODO

- Automate validation by writing a `run-tests.sh`.

## Completed Items

- Add support to exit after reading the specified number of records.
