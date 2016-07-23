/*
*This class for send every Second a Word to specified topic
*i will with this want test windowing. 
 */
package com.datis.producer;

import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author jeus
 */
public class Region extends Thread {

    Properties propr = new Properties();
    String[] world = "USA,Afghanistan,Albania,Algeria".split(",");
    KafkaProducer<Long, String> producer;

    public void Region() {
        propr = new Properties();
        propr.put("bootstrap.server", "172.17.0.13:9092");
        propr.put("client.id", "region");
//        props.put("batch.size",150);//this for async by size in buffer
//        props.put("linger.ms", 9000);//this for async by milisecond messages buffered
        propr.put("acks", "1");
        propr.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        propr.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    @Override
    public void run() {
        producer = new KafkaProducer<>(propr);
        //send data by sync data to consumer; //if not send Data to topic try again 
        while (true) {

            try {

                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Region.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private String getWord() {
        Random random = new Random();
        return world[random.nextInt(world.length)];
    }

}

class RegionCallback implements Callback {

    private final long startTime;
    private final int key;
    private final String messages;

    public RegionCallback(long stTime, int keyCode, String msg) {
        this.startTime = stTime;
        this.key = keyCode;
        this.messages = msg;
    }
    
    @Override
    public void onCompletion(RecordMetadata arg0, Exception arg1) {
        if (arg0 != null) {

        } else {
        }

    }

}
