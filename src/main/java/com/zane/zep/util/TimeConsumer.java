package com.zane.zep.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;

public class TimeConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.64.129:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        String topic = "testtopic";
        Calendar c = Calendar.getInstance();
        c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)-2);
        System.out.println("Start Time = "+c.getTime().toString());
        long timestampBeginning =  c.getTimeInMillis();

        Calendar e = Calendar.getInstance();
        e.set(Calendar.MINUTE, e.get(Calendar.MINUTE)-1);
        System.out.println("End Time = "+e.getTime().toString());
        long timestampEnd = e.getTimeInMillis();
        TopicPartition partition = new TopicPartition(topic, 0);
        System.out.println("Partition "+partition);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        long beginningOffset = consumer.offsetsForTimes(
                Collections.singletonMap(partition, timestampBeginning))
                .get(partition).offset();
        System.out.println("S:"+timestampBeginning+",E:"+timestampEnd);
        System.out.println("Offset Start:"+beginningOffset);
        consumer.assign(Collections.singleton(partition)); // must assign before seeking
        consumer.seek(partition, beginningOffset);

        for (ConsumerRecord<String, String> record : consumer.poll(100)) {
            System.out.println("Record: "+record.key()+"-"+record.partition()+"-"+record.offset()+":"+record.value());
            if (record.timestamp() > timestampEnd) {
                break; // or whatever
            }

            // handle record
        }
    }
}
