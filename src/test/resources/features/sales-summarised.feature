Feature: Sales Summarised.

  Scenario: A sale-summary is produced for each placed order.

  Scenario Outline:
    Given An existing customer of "<customer>".
    And A product catalog of "<product-catalog>".
    When Order "<order>" is placed.
    Then The Sales Team are notified of Order "<summary>".
    Scenarios:
      | customer   | product-catalog    | order       | summary             |
      | mary-jones | lemons-and-bananas | marys-order | marys-order-summary |
      | john-smith | apples-and-pears   | johns-order | johns-order-summary |
