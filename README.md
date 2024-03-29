# Problem

We are building a search bar that lets people do fuzzy search on different Konnect entities (services, routes, nodes).
you're in charge of creating the backend ingest to power that service built on top of a [CDC stream](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events) generated by Debezium

We have provided a jsonl file containing some sample events that can be used to
simulate input stream.


Below are the tasks we want you to complete.

* develop a program that ingests the sample cdc events into a Kafka topic
* develop a program that persists the data from Kafka into Opensearch


## Get started

Run

```
docker-compose up -d
```

to start a Kakfa cluster.

The cluster is accessible locally at `localhost:9092` or `kafka:29092` for services running inside the container network.


You can also access Kafka-UI at `localhost:8080` to examine the ingested Kafka messages.

Opensearch is accessible locally at `localhost:9200` or `opensearch-node:9200`
for services running inside the container network.

You can validate Opensearch is working by running sample queries

Insert
```
curl -X PUT localhost:9200/cdc/_doc/1 -H "Content-Type: application/json" -d '{"foo": "bar"}'
{"_index":"cdc","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}%
```

Search
```
curl localhost:9200/cdc/_search  | python -m json.tool
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   223  100   223    0     0  41527      0 --:--:-- --:--:-- --:--:-- 44600
{
    "took": 1,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 1,
            "relation": "eq"
        },
        "max_score": 1.0,
        "hits": [
            {
                "_index": "cdc",
                "_id": "1",
                "_score": 1.0,
                "_source": {
                    "foo": "bar"
                }
            }
        ]
    }
}
```

Run

```
docker-compose down
```

to tear down all the services.

## Resources

* `stream.jsonl` contains cdc events that need to be ingested
* `docker-compose.yaml` contains the skeleton services to help you get started

# Solution

#Install Java
```shell
brew install openjdk
```

Verify the installation

```shell
java -version
```

```text
openjdk version "21.0.2" 2024-01-16
OpenJDK Runtime Environment (build 21.0.2+13-58)
OpenJDK 64-Bit Server VM (build 21.0.2+13-58, mixed mode, sharing)
```

# Install Maven
```shell
brew install maven
```
Verify the installation

```
mvn --version
```

You'll get output similar to the following:

```text
Apache Maven 3.9.6 (bc0240f3c744dd6b6ec2920b3cd08dcc295161ae)
Maven home: /opt/homebrew/Cellar/maven/3.9.6/libexec
Java version: 21.0.2, vendor: Homebrew, runtime: /opt/homebrew/Cellar/openjdk/21.0.2/libexec/openjdk.jdk/Contents/Home
Default locale: en_IN, platform encoding: UTF-8
OS name: "mac os x", version: "14.3.1", arch: "aarch64", family: "mac"
```
# Starting the file based producer

In a new terminal window, go to the directory in which this code is installed and execute the following command:
(This reads entry from **stream.jsonl file in src/main/java/resources**)
```shell
sh runProducer.sh
```

You see a steady stream of screen output that is the console output of messages being sent to the topic named `cdc-events`.

# Starting a consumer

In another terminal window, go to the directory in which this code is installed and execute the following command:

```shell
sh runConsumer.sh
```

You see a steady stream of screen output that is the log output of messages being retrieved from the topic named `cdc-events`.

You can validate Opensearch is working by running sample queries

Search
```
curl localhost:9200/cdc/_search  | python -m json.tool
```