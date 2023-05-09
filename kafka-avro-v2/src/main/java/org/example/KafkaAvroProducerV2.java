package org.example;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.example.Constant.SCHEMA_REGISTRY_URL_KEY;

/**
 * Hello world!
 */
public class KafkaAvroProducerV2 {
    public static final String TOPIC = "customer-avro";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, Constant.ACKS);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Constant.RETRIES);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL_KEY, Constant.SCHEMA_REGISTRY_URL);

        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties)) {
            Customer customer = Customer.newBuilder()
                    .setFirstName("Jon")
                    .setLastName("Doe")
                    .setAge(31)
                    .setHeight(180.0f)
                    .setWeight(85.5f)
                    .setEmail("e@mail.com")
                    .setPhoneNumber("phone-num-ber")
                    .build();

            ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(
                    TOPIC, customer
            );

            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    System.out.println(recordMetadata.toString());
                } else {
                    System.out.println(recordMetadata.toString() + "-" + e);
                }
            });

            kafkaProducer.flush();
        }
    }
}
