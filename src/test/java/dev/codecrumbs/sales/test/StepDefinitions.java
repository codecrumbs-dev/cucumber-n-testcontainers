package dev.codecrumbs.sales.test;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

public class StepDefinitions {
    private static final Logger LOG = LoggerFactory.getLogger(StepDefinitions.class);
    private int orderNumber;

    @Given("An existing customer of {string}.")
    public void anExistingCustomer(String customer) throws SQLException {
        LOG.info("Loading existing customer {}", customer);
        Pipeline.insertCustomer(
                Utils.loadJson(String.format("/test-data/customers/%s.json", customer))
        );
    }

    @And("A product catalog of {string}.")
    public void aProductCatalogOf(String catalog) throws SQLException {
        LOG.info("Loading product catalog {}", catalog);
        Pipeline.insertProducts(
                Utils.loadJson(String.format("/test-data/product-catalogs/%s.json", catalog))
        );
    }

    @When("Order {string} is placed.")
    public void orderIsReceived(String order) {
        orderNumber = ThreadLocalRandom.current().nextInt(1000000);
        LOG.info("Injecting order {} with order number {}", order, orderNumber);
        Pipeline.send(
                Topic.ORDERS,
                orderNumber,
                Utils.loadJson(String.format("/test-data/orders/%s.json", order))
        );
    }

    @Then("The Sales Team are notified of Order {string}.")
    public void salesTeamAreNotifiedOfOrder(String orderSummary) {
        LOG.info("Verifying that order summary {} is received by sales team.", orderSummary);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertThat(Pipeline.messagesOn(Topic.SALES))
                        .hasSize(1)
                        .containsOnlyKeys(orderNumber)
                        .containsValue(Utils.loadJson(String.format("/test-data/sales-summaries/%s.json", orderSummary)))
        );
    }
}
