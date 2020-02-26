package com.zane.zep.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Future;

public class Producer {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.64.129:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        List<Future<RecordMetadata>> futures = new ArrayList<Future<RecordMetadata>>();
        try{
            for(int i = 0; i < 1000; i++){
                System.out.println("Sending Message tp:"+i);
                Thread.sleep(10);
                Future<RecordMetadata> f = kafkaProducer.send(new ProducerRecord<String, String>(
                        "testtopic", "IP:" + InetAddress.getLocalHost().getHostAddress() + " - Message:" +
                          new Date().toString()+ "_"+UUID.randomUUID().toString()));
                if (i% 20 == 0) {
                    futures.add(f);
                }
            }

            for (Future f:futures
                 ) {
                RecordMetadata r = (RecordMetadata) f.get();
                System.out.println("Record:"+r.offset()+":"+r.partition()+":"+r.timestamp());
            }


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
