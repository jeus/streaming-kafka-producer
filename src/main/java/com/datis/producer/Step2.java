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
public class Step2 extends Thread {

    private final String topic;
    Properties propr = new Properties();
    String[] world = "USA,Afghanistan,Albania,Algeria".split(",");
    KafkaProducer<String, Long> producer;

    public Step2() {
        topic = "step344443";
        propr = new Properties();
        propr.put("bootstrap.servers", "172.17.0.13:9092");
        propr.put("client.id", "step2");
//        props.put("batch.size",150);//this for async by size in buffer
//        props.put("linger.ms", 9000);//this for async by milisecond messages buffered
        propr.put("acks", "1");
        propr.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propr.put("value.serializer","org.apache.kafka.common.serialization.LongSerializer");
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
                Step2Callback regCallBack = new Step2Callback(getWord(),dt.getTime());
                RecordMetadata rc = producer.send(new ProducerRecord<String,Long>(topic, getWord(),key), regCallBack).get();
                System.out.println("Send Data To Topic Sync:" + rc.offset() + "   Str:" + rc.toString());

                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Step2.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ExecutionException ex) {
                Logger.getLogger(Step2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String[] args) {
        Step2 reg = new Step2();
        reg.start();
    }

    private String getWord() {
        Random random = new Random();
        return world[random.nextInt(world.length)];
    }

}

class Step2Callback implements Callback {

    private final Long key;
    private final String message;

    public Step2Callback(String msg,long stTime) {
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
