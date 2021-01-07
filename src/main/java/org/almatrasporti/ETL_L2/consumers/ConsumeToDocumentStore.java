package org.almatrasporti.ETL_L2.consumers;

import org.almatrasporti.ETL_L2.writers.IWriter;
import org.almatrasporti.common.utils.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumeToDocumentStore {
    private final List<IWriter> writerList;
    private Properties props;
    private KafkaConsumer<String, String> consumer;
    protected String inputTopic = Config.getInstance().get("Input.topic");

    public ConsumeToDocumentStore(List<IWriter> writerList) {
        this.writerList = writerList;

        props = new Properties();
        props.setProperty("bootstrap.servers", Config.getInstance().get("Kafka.servers")  != null ? Config.getInstance().get("Kafka.servers") : "localhost:9092");
        props.setProperty("group.id", "mongo_consumer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(this.inputTopic));
    }

    public void consume() {
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                writerList.stream().forEach(writer -> writer.upsertRecord(record.value()));
            }
        }
    }
}
