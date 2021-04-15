package dev.codecrumbs.sales.test;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

public class StepDefinitions {
    private static final Logger LOG = LoggerFactory.getLogger(StepDefinitions.class);
    private int orderNumber;

    @Given("An existing customer {string}.")
    public void anExistingCustomer(String customer) {
        LOG.info("Loading existing customer {}", customer);
    }

    @And("A product catalog of {string}.")
    public void aProductCatalogOf(String catalog) {
        LOG.info("Loading product catalog {}", catalog);
    }

    @When("Order {string} is placed.")
    public void orderIsReceived(String order) {
        orderNumber = ThreadLocalRandom.current().nextInt(1000000);
        LOG.info("Injecting order {} with order number {}", order, orderNumber);
    }

    @Then("The Sales Team are notified of Order {string}.")
    public void salesTeamAreNotifiedOfOrder(String orderSummary) {
        LOG.info("Verifying that order summary {} is received by sales team.", orderSummary);
    }
}
