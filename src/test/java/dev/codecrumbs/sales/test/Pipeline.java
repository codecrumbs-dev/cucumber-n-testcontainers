package dev.codecrumbs.sales.test;

import com.fasterxml.jackson.databind.JsonNode;
import io.cucumber.plugin.EventListener;
import io.cucumber.plugin.event.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Pipeline implements EventListener {

    private static final String CONFLUENT_VERSION = "6.0.1";
    private static final String MARIADB_VERSION = "10.5.9";

    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                    .withTag(CONFLUENT_VERSION))
            .withLogConsumer(logConsumer("kafka-container"))
            .waitingFor(Wait.forLogMessage(".*KafkaServer.*started.*", 1));


    private static final MariaDBContainer<?> DATABASE = new MariaDBContainer<>(
            DockerImageName.parse("mariadb")
                    .withTag(MARIADB_VERSION))
            .withInitScript("schema.sql")
            .withLogConsumer(logConsumer("maria-db"));

    private static final GenericContainer<?> APPLICATION =
            new GenericContainer<>(DockerImageName.parse("order-enrichment-stream"))
                    .withLogConsumer(logConsumer("order-enrichment-stream"));

    private static Slf4jLogConsumer logConsumer(String loggerName) {
        return new Slf4jLogConsumer(LoggerFactory.getLogger(loggerName)).withSeparateOutputStreams();
    }

    private static KafkaConsumer<Integer, String> kafkaConsumer;

    private final EventHandler<TestRunStarted> startUp = testRunStarted -> {
        Network network = Network.newNetwork();
        KAFKA.withNetwork(network).start();
        DATABASE.withNetwork(network).start();
        APPLICATION.withNetwork(network)
                .withEnv(
                        Map.of(
                                "spring.kafka.bootstrapServers", KAFKA.getNetworkAliases().get(0) + ":9092",
                                "spring.cloud.stream.kafka.binder.brokers", KAFKA.getNetworkAliases().get(0) + ":9092",
                                "spring.cloud.stream.bindings.enrichOrder-in-0.destination", Topic.ORDERS.value(),
                                "spring.cloud.stream.bindings.enrichOrder-out-0.destination", Topic.SALES.value(),
                                "spring.datasource.url", "jdbc:mariadb://" + DATABASE.getNetworkAliases().get(0) + ":3306/" + DATABASE.getDatabaseName(),
                                "spring.datasource.username", DATABASE.getUsername(),
                                "spring.datasource.password", DATABASE.getPassword()
                        )
                )
                .waitingFor(Wait.forLogMessage(".*Started Application.*", 1))
                .start();
        Utils.createTopics(KAFKA);
        kafkaConsumer = Utils.consumerFor(Topic.SALES, KAFKA);
    };

    private final EventHandler<TestCaseStarted> startTestCase = testCaseStarted -> {
        Utils.jdbcTemplateFor(DATABASE).update("delete from customers");
        Utils.jdbcTemplateFor(DATABASE).update("delete from products");
    };

    private final EventHandler<TestRunFinished> shutDown = testRunFinished -> {
        KAFKA.stop();
        DATABASE.stop();
        APPLICATION.stop();
    };

    public static void send(Topic topic, int orderNumber, JsonNode document) {
            Utils.send(
                    KAFKA,
                    new ProducerRecord<>(
                            topic.value(),
                            orderNumber,
                            document.toString()
                    )
            );
    }

    public static Map<Integer, JsonNode> messagesOn(Topic topic) {
        return StreamSupport.stream(
                KafkaTestUtils.getRecords(kafkaConsumer)
                        .records(topic.value()).spliterator(), false
        ).collect(Collectors.toMap(ConsumerRecord::key, r -> Utils.toJson(r.value())));
    }

    public static void insertCustomer(JsonNode customer) throws SQLException {
        Utils.jdbcTemplateFor(DATABASE).update(
                "insert into customers (id, name) values (?, ?)",
                customer.get("id").intValue(),
                customer.get("name").textValue()
        );
    }

    public static void insertProducts(JsonNode productCatalog) throws SQLException {
        JdbcTemplate jdbcTemplate = Utils.jdbcTemplateFor(DATABASE);
        for (JsonNode eachProduct : productCatalog) {
            jdbcTemplate.update(
                    "insert into products (id, description, unit_price) values (?, ?, ?)",
                    eachProduct.get("id").intValue(),
                    eachProduct.get("item").textValue(),
                    eachProduct.get("unitPrice").decimalValue()
            );
        }
    }

    @Override
    public void setEventPublisher(EventPublisher publisher) {
        publisher.registerHandlerFor(TestRunStarted.class, startUp);
        publisher.registerHandlerFor(TestCaseStarted.class, startTestCase);
        publisher.registerHandlerFor(TestRunFinished.class, shutDown);
    }
}
