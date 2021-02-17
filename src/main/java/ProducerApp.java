
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private int counter;
    private String KAFKA_BROKER_URL = "localhost:9092";
    private String TOPIC_NAME = "testTopic";
    private String clientID = "client_prod_1";

    public static void main(String[] args) {
        new ProducerApp();
    }

    public ProducerApp() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER_URL);
        properties.put("client.id", clientID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            ++counter;
            String msg = String.valueOf(Math.random() * 1000);
            producer.send(new ProducerRecord<Integer, String>(TOPIC_NAME, ++counter, msg),
                    (metadata, ex) -> {
                        System.out.println("Sending Message key=>"
                                + counter
                                + " Value =>"
                                + msg);
                        System.out.println("Partition => " + metadata.partition() + " Offset=>" + metadata.offset());
                    });
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }
}