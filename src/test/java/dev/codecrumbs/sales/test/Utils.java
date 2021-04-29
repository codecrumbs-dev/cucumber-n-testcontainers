package dev.codecrumbs.sales.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MariaDBContainer;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Utils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static JsonNode loadJson(String document) {

        try {
            return OBJECT_MAPPER.readTree(
                    Files.readString(
                            Paths.get(
                                    Objects.requireNonNull(Utils.class.getResource(document)).toURI()
                            ),
                            Charset.defaultCharset()
                    )
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static JsonNode toJson(String value) {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static JdbcTemplate jdbcTemplateFor(MariaDBContainer<?> database) {
        try {
            MariaDbDataSource dataSource = new MariaDbDataSource(database.getJdbcUrl());
            dataSource.setUser(database.getUsername());
            dataSource.setPassword(database.getPassword());
            return new JdbcTemplate(dataSource);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static KafkaConsumer<Integer, String> consumerFor(Topic topic, KafkaContainer kafka) {

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(
                KafkaTestUtils.consumerProps(
                        kafka.getBootstrapServers(),
                        "any-group-" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE),
                        "true"
                )
        );
        consumer.subscribe(Set.of(topic.value()));
        return consumer;
    }

    static Producer<Integer, String> producerFor(KafkaContainer kafka) {
        return new KafkaProducer<>(KafkaTestUtils.producerProps(kafka.getBootstrapServers())
        );
    }

    static void createTopics(KafkaContainer kafka) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        AdminClient adminClient = AdminClient.create(properties);

        adminClient.createTopics(
                Arrays.stream(Topic.values())
                        .map(topic -> new NewTopic(topic.value(), 1, (short) 1))
                        .collect(Collectors.toList())
        );
        adminClient.close();
    }

    public static void send(KafkaContainer kafka, ProducerRecord<Integer, String> record) {
        try {
            Producer<Integer, String> producer = Utils.producerFor(kafka);
            Future<RecordMetadata> future = producer.send(record);
            future.get();
            producer.close();
        } catch (Exception e) {
            throw new RuntimeException("Error sending message", e);
        }
    }
}
