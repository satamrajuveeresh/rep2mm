package com.veeresh.rep2mm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import java.util.Properties;


public class MyConsumer {

    @Value("${bootstrapservers}")
    private String bootstrapServers;

    @Value("${groupid}")
    private String groupId;

    public Properties consumeProperties(){
        Properties properties=new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //org.apache.kafka.connect.json.JsonConverter
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //  properties.setProperty(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }

    public Properties myProduceProperties(){
        Properties properties1=new Properties();

        properties1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //org.apache.kafka.connect.json.JsonConverter
        properties1.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties1.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties1.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        return properties1;
    }
}
