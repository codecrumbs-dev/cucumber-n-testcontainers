CREATE TABLE products
(
    id          BIGINT UNSIGNED PRIMARY KEY,
    description VARCHAR(30),
    unit_price  DECIMAL(6, 2)
);

CREATE TABLE customers
(
    id   BIGINT UNSIGNED PRIMARY KEY,
    name VARCHAR(50)
);