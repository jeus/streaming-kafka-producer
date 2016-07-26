/*
*This class for send every Second a Word to specified topic
*i will with this want test windowing. 
 */
package com.datis.producer;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author jeus
 */
public class Region extends Thread {

    private final String topic;
    Properties propr = new Properties();
    String[] world = "USA,Afghanistan,Albania,Algeria".split(",");
    KafkaProducer<Long, String> producer;

    public Region() {
        topic = "step1";
        propr = new Properties();
        propr.put("bootstrap.servers", "172.17.0.13:9092");
        propr.put("client.id", "region");
        propr.put("buffer.memory", 33554432);
//        propr.put("batch.size",800);//this for async by size in buffer
        propr.put("linger.ms",  9000);//this for async by milisecond messages buffered
        propr.put("acks", "1");
        propr.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        propr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(propr);
    }

    @Override
    public void run() {
        System.out.println("THIS START");
        //send data by sync data to consumer; //if not send Data to topic try again 
        Date dt;
        while (true) {
            dt = new Date();
            Long key = dt.getTime();
            try {
                RegionCallback regCallBack = new RegionCallback(dt.getTime(), getWord());
//                RecordMetadata rc =
                        producer.send(new ProducerRecord<>(topic, key, getWord()), regCallBack);
//                System.out.println("Send Data To Topic Sync:" + rc.offset() + "   Str:" + rc.toString());

                Thread.sleep(3000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Region.class.getName()).log(Level.SEVERE, null, ex);
//            } catch (ExecutionException ex) {
//                Logger.getLogger(Region.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String[] args) {
        Region reg = new Region();
        reg.start();
    }

    private String getWord() {
        Random random = new Random();
        return world[random.nextInt(world.length)];
    }

}

class RegionCallback implements Callback {

    private final Long key;
    private final String message;

    public RegionCallback(long stTime, String msg) {
        this.key = stTime;
        this.message = msg;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
                    + "), " + "offset(" + metadata.offset() + ")");
        } else {
            exception.printStackTrace();
        }

    }

}
