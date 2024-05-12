# Parquet to Kafka
This sub-project is responsible of detecting and ingesting the content of generated log files to a Kafka instance.

## Prerequisites
- Zookeeper, Kafka and Nifi properly installed on your machine. Here are some links which can help:
    
    * Kafka installation: https://kafka.apache.org/downloads
    * Nifi installation: https://nifi.apache.org/download/

- Once all installations are done, make sure to add the kafka bin folder to your path to facilitate command line itneractions and start by running the following command to start Zookeeper:

    * zookeeper-server-start.bat path-to-kafka\config\zookeeper.properties

- Then, you need to start Kafka server:

    * kafka-server-start.bat path-to-kafka\config\server.properties

- Once started, you need to create the topic in which we'll be saving the data:

    * kafka-topics.bat --bootstrap-server localhost:9092 --topic loganalytics --create --partitions 3 --replication-factor 1

- Now, you can start Nifi (make sure to add Nifi's bin folder also to your path):

    * run-nifi.bat

- You can visit this URL: https://localhost:8443/nifi/login to see your Nifi instance. Once logged in, you need to import "ParquetToKafka.json" template and start it.

If you keep the instance running and without stopping the processors, this will guarantee that the ingestion will be automatic and every parquet file that will fall in the "logs" folder will be ingested almost instantly to the Kafka topic.