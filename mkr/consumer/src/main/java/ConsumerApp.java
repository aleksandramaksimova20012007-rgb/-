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

public class ConsumerApp {

    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String REQUEST_TOPIC = "demo-requests";
    private static final String RESPONSE_TOPIC = "demo-responses";
    private static final String GROUP_ID = "demo-responder-group";

    public static void main(String[] args) throws Exception {
        createTopics();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties());

        consumer.subscribe(Collections.singletonList(REQUEST_TOPIC));

        System.out.println("Чекаю запитів у '" + REQUEST_TOPIC + "'.");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                String request = record.value();
                String[] parts = request.split(",");

                long start = Long.parseLong(parts[0].trim());
                long finish = Long.parseLong(parts[1].trim());

                System.out.println("<- Отримано запит: start=" + start + " finish=" + finish);

                double avgSteps = calculateAverageSteps(start, finish);

                Header correlationHeader = record.headers().lastHeader("correlation-id");

                if (correlationHeader == null) {
                    System.out.println("Запит без correlation-id, пропускаю.");
                    continue;
                }

                byte[] correlationId = correlationHeader.value();

                ProducerRecord<String, String> response =
                        new ProducerRecord<>(RESPONSE_TOPIC, String.valueOf(avgSteps));

                response.headers().add(new RecordHeader("correlation-id", correlationId));

                producer.send(response).get();

                System.out.println("-> Надіслано відповідь: avgSteps=" + avgSteps);
            }
        }
    }

    private static double calculateAverageSteps(long start, long finish) {
        long totalSteps = 0;
        long count = 0;

        for (long i = start; i <= finish; i++) {
            totalSteps += countCollatzSteps(i);
            count++;
        }

        return (double) totalSteps / count;
    }

    private static int countCollatzSteps(long number) {
        int steps = 0;

        while (number != 1) {
            if (number % 2 == 0) {
                number /= 2;
            } else {
                number = 3 * number + 1;
            }

            steps++;
        }

        return steps;
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

    private static Properties consumerProperties() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        return props;
    }

    private static Properties producerProperties() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}