# Test Kafka Environment

These instructions help setup a light-weight Zookeeper and Kafka environment
on the local box.

## Platform

Steps in this document have been tried on CentOS 7.

## Installation

Follow these instructions if you do not have `docker` and `docker-compose`
installed on the system:

```shell
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install docker-ce docker-compose
```

## Running

The docker-compose file has been created by using the
[documentation](https://hub.docker.com/r/bitnami/kafka/) from Bitnami and
experimentation. Bring up Kafka via `docker-compose`:

```shell
docker-compose up
```

Press Ctrl-c to shutdown.
