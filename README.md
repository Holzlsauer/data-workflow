# data-workflow

## Kafka setup

[Medium - Kafka cluster with docker](https://medium.com/@erkndmrl/kafka-cluster-with-docker-compose-5864d50f677e)

## Airflow setup

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

## Spark

[Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

```sh
docker container exec -it spark /bin/bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 scripts/main.py
```

## MongoDB
