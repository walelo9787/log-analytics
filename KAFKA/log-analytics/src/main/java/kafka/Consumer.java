package kafka;

import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class.getName());
    private final String topic;
    private final String server;
    private final String keyDeserilizer;
    private final String valueDeserilizer;
    private final String groupId;
    private final String offsetResetConfig;
    private final Properties properties;
    private final KafkaConsumer<String, String> consumer;


    public Consumer(
        final String topic, 
        final String server, 
        final String keyDeserilizer, 
        final String valueDeserializer,
        final String groupId,
        final String offsetReset) {
        this.topic = topic;
        this.server = server;
        this.keyDeserilizer = keyDeserilizer;
        this.valueDeserilizer = valueDeserializer;
        this.groupId = groupId;
        this.offsetResetConfig = offsetReset;

        this.properties = initProperties();

        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(this.topic));
        LOGGER.info("Consumer created : [topic={}] [server={}] [groupId={}]", topic, server, groupId);
    }

    private Properties initProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        props.put("group.id", groupId);
        props.put("key.deserializer", keyDeserilizer);
        props.put("value.deserializer", valueDeserilizer);
        props.put("auto.offset.reset", offsetResetConfig);
        props.put("offsets.retention.minutes", "0");
        
        return props;
    }

    public Set<String> consume() {
        long startTime = System.currentTimeMillis();
        long elapsed = 0;
        Set<String> readRecords = new HashSet<>();
        int i = 1;
        try {
            // loop for 5 seconds
            while (elapsed < 5000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

                for (ConsumerRecord<String, String> record : records) {
                    readRecords.add(record.value());
                    LOGGER.info("Read {} records from topic {}", i, topic);
                    i += 1;
                }

                long afterIterationTime = System.currentTimeMillis();
                elapsed = afterIterationTime - startTime;
            }
        } finally {
            consumer.close();
        }

        return readRecords;
    }
}
