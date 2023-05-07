package org.example;

import com.example.CustomerV1;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.example.Constant.SCHEMA_REGISTRY_URL;
import static org.example.Constant.SCHEMA_REGISTRY_URL_KEY;

/**
 * Hello world!
 */
public class KafkaAvroProducerV1 {


    public static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, Constant.ACKS);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Constant.RETRIES);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL_KEY, Constant.SCHEMA_REGISTRY_URL);

        try (KafkaProducer<String, CustomerV1> kafkaProducer = new KafkaProducer<>(properties);) {
            CustomerV1 customer = CustomerV1.newBuilder()
                    .setFirstName("Jon")
                    .setLastName("Doe")
                    .setAge(31)
                    .setHeight(180.0f)
                    .setWeight(85.5f)
                    .build();

            ProducerRecord<String, CustomerV1> producerRecord = new ProducerRecord<String, CustomerV1>(
                    TOPIC, customer
            );

            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        System.out.println(recordMetadata.toString());
                    } else {
                        System.out.println(recordMetadata.toString() +"-"+e);
                    }
                }
            });

            kafkaProducer.flush();
        }
    }
}
