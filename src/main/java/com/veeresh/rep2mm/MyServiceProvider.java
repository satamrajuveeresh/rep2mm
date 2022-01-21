package com.veeresh.rep2mm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Properties;

@Service
public class MyServiceProvider {

    @Value("${bootstrapservers}")
    private String bootstrapServers;

    @Value("${groupid}")
    private String groupId;

    @Value("${offsettopic}")
    private String offsetTopic;

    @Value("${replicatorName}")
private String replicatorName;

    @Value("${mm2Name}")
    private String mm2Name;

    @Value("${processType}")
    private String processType;

@Bean
    public void evaluteprocess(){
        if(processType.equalsIgnoreCase("mm2")){
            writeMM2();
            System.exit(0);
        }else if(processType.equalsIgnoreCase("replicator")){
            write2Rep();
            System.exit(0);
        }else {
            System.out.println("define a proper execution process. Due to lack of this execution system would exit now");
            System.exit(143);
        }
    }


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
public void writeMM2(){
    KafkaConsumer<String,String> consumer= new KafkaConsumer<>(consumeProperties());
    Producer<String, String> producer = new KafkaProducer<String, String>(myProduceProperties());
    consumer.subscribe(Arrays.asList(offsetTopic));
    while(true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        if (records.isEmpty()) {
            System.exit(0);
        }
        else {
        for (ConsumerRecord<String, String> record : records) {
                    System.out.println("record"+ record.key()+"value"+ record.value());
                if ((record.key().contains(replicatorName))) {
                    calltoProducer(mm2keyGen(record.key()), record.value());
                    //producer.send(new ProducerRecord<String, String>(offsetTopic,mm2keyGen(record.key()) , record.value()));
                }
            }
        }
    }
    }

    public void write2Rep(){
        KafkaConsumer<String,String> consumer= new KafkaConsumer<>(consumeProperties());

        consumer.subscribe(Arrays.asList(offsetTopic));
       while(true) {
           ConsumerRecords<String, String> records = consumer.poll(1000);
           if (records.isEmpty()) {
               System.exit(0);
           } else {
           for (ConsumerRecord<String, String> record : records) {
               System.out.println("record"+ record.key()+"value"+ record.value());
                   if ((record.key().contains(mm2Name))) {
                       calltoProducer(repKeyGen(record.key()), record.value());

                   }
               }
           }
       }
    }

public void calltoProducer(String key,String value){
    Producer<String, String> producer = new KafkaProducer<String, String>(myProduceProperties());
    System.out.println("new record"+ key+"value"+ value);
    producer.send(new ProducerRecord<String, String>(offsetTopic,key , value));
}

    private String mm2keyGen(String s1){
    System.out.println(s1);
        String s11 = s1.replace("[", "").replace("]", "").replace("{", "").replace("}", "");
        String sa[] = s11.split(",");
        //String ks = "[\"mm2-msc\",{\"cluster\":\"\",\"partition\":0,\"topic\":\"topic2\"}]";
        String returnString="["+"\""+mm2Name+"\""+","+"{"+"\"cluster\":\"\","+sa[2]+","+sa[1]+"}]";

        return returnString;
    }

    private String repKeyGen(String s1){
        System.out.println(s1);
        String s11 = s1.replace("\"cluster\":\"\",","").replace("[", "").replace("]", "").replace("{", "").replace("}", "");
        String sa[] = s11.split(",");

        String finalString="["+"\""+replicatorName+"\""+","+"{"+sa[2]+","+sa[1]+"}]";
        return finalString;
    }




}
