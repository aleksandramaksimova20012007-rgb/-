import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class ProducerApp {

    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";

    public static void main(String[] args) throws Exception {
        createTopics();

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties());

        consumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));

        String correlationId = UUID.randomUUID().toString();

        long start = 10;
        long finish = 100;

        String message = start + "," + finish;

        ProducerRecord<String, String> request =
                new ProducerRecord<>(REQUEST_TOPIC, message);

        request.headers().add(
                new RecordHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8))
        );

        producer.send(request).get();

        System.out.println("-> Запит надіслано: start=" + start + " finish=" + finish + " (id=" + correlationId + ")");

        boolean responseReceived = false;
        long timeoutMs = 3_000_000;
        long startTime = System.currentTimeMillis();

        while (!responseReceived && System.currentTimeMillis() - startTime < timeoutMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                Header header = record.headers().lastHeader("correlation-id");

                if (header == null) {
                    continue;
                }

                String responseCorrelationId =
                        new String(header.value(), StandardCharsets.UTF_8);

                if (correlationId.equals(responseCorrelationId)) {
                    System.out.println("<- Отримано відповідь: avgSteps=" + record.value());
                    responseReceived = true;
                    break;
                }
            }
        }

        if (!responseReceived) {
            System.out.println("Відповідь не отримано за таймаут.");
        }

        System.out.println("Готово. Контейнер живе.");

        while (true) {
            Thread.sleep(60_000);
        }
    }

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic requests = new NewTopic(REQUEST_TOPIC, 1, (short) 1);
            NewTopic responses = new NewTopic(RESPONSE_TOPIC, 1, (short) 1);

            try {
                admin.createTopics(java.util.List.of(requests, responses)).all().get();
            } catch (Exception ignored) {
            }
        }
    }

    private static Properties producerProperties() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    private static Properties consumerProperties() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "demo-producer-group-" + UUID.randomUUID());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        return props;
    }
}