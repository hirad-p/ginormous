package spring2018.lab4;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;

/**
 * @author jcasaletto
 * 
 * Consumes syslog messages from input Kafka topic, looks for alerts, then produces records to output Kafka topic
 *
 * Usage: Syslog2Alert <broker> <master> <in-topic> <out-topic> <cg> <interval> <threshold>
 *   <broker> is one of the servers in the kafka cluster
 *   <master> is either local[n] or yarn
 *   <in-topic> is the kafka topic to consume from
 *   <out-topic> is the kafka topic to produce to
 *   <cg> is the consumer group name
 *   <interval> is the number of milliseconds per batch
 *   <threshold> is the integer syslog level at which alerts are generated
 *
 */
public final class Syslog2Alert {
    public static void main(String[] args) {
        if (args.length < 7) {
            System.err.println("Usage: Syslog2Alert <kafka-broker> <deploy-endpoint> <in-topic> <out-topic> <cg> <interval> <threshold>");
            System.err.println("eg: Syslog2Alert cs185:9092 local[*] test out mycg 5000 3");
            System.exit(1);
        }

        // set variables from command-line arguments
        final String broker = args[0];
        String deployEndpoint = args[1];
        String inTopic = args[2];
        final String outTopic = args[3];
        String consumerGroup = args[4];
        long interval = Long.parseLong(args[5]);
        int threshold = Integer.parseInt(args[6]);
        
        // define topic to subscribe to
        final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);
    
        // set Kafka consumer parameters
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        // TODO: populate the kafkaParams map with the following values:
        // key.deserializer, value.deserializer, bootstrap.servers, and group.id

        // initialize the streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(deployEndpoint, "Syslog2Alert", 
            new Duration(interval));

        // pull ConsumerRecords out of the stream
        JavaInputDStream<ConsumerRecord<String, String>> messages = 
                        KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>SubscribePattern(topicPattern, kafkaParams)
                      );
        //messages.print();
    
        // TODO: implement the map() function to pull values out of ConsumerRecords 
        JavaDStream<String> values = null;
        //values.print();
   
        // TODO: implement the filter() function to filter messages less than or equal to threshold
        JavaDStream<String> alertMessages = null;
        
        // TODO: implement the call() method to send the alerts to the output stream
        alertMessages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 2700738329774962618L;
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    private static final long serialVersionUID = -250139202220821945L;
                    @Override
                    public void call(Iterator<String> iterator) throws Exception {
                        // TODO: configure producer properties 
                        // including bootstrap.servers, key.serializer, and value.serializer
                        Properties producerProps = new Properties();
                       
                        
                        // TODO: instantiate the producer once per partition -- is there a better way?
                        KafkaProducer<String, JsonNode> producer = null;
                              
                        // TODO: iterate through the iterator, create and prodcue JSON record value (use null key)
                        while(iterator.hasNext()) {
                           
                        }
                        // close the producer per partition
                        producer.close();                              
                    }               
                });  
            }
        }); 
        
        // start the consumer
        jssc.start();
        
        // stay in infinite loop until terminated
        try {
            jssc.awaitTermination();
        } 
        catch (InterruptedException e) {
        }
    }
}
