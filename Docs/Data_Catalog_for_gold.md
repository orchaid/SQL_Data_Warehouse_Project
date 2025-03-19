# Data Dictionary for Gold Layer
## Overview
gold.dim_customers Data Catalog

### 1. gold.dim_customers

- **Purpose**: Stores the details about the customers

- **columns**:


| Column Name     | Data Type     | Description                                                                                               |
| :-------------- | :------------ | :-------------------------------------------------------------------------------------------------------- |
| customer_key    | INT           | Surrogate identifing the customer dimension. Example: 12345                                              |
| customer_id     | INT  | Unique identifier assigned to each row. Example: "CUST_9876"                                            |
| customer_number | VARCHAR(50)   | Customer number in the company system.                                               |
| first_name      | VARCHAR(50)  | Customer's first name.                                                                   |
| last_name       | VARCHAR(50)  | Customer's last name.                                                                    |
| country         | VARCHAR(50)   | Customer's country of residence. Example: "Germany"                                                           |
| marital_status  | VARCHAR(20)   | Customer's marital status. Example: ("Married","Single")                                                             |
| gender          | VARCHAR(10)   | Customer's gender. Example: ("Female" , "Male")                                                                     |
| birth_date      | DATE          | Customer's date of birth. Example: "1990-01-15"                                                           |
| create_date     | TIMESTAMP     | Date when the customer record was created. Example: "2023-10-27 10:30:00"                                 |



### 2. gold.dim_products

- **Purpose**: Stores the details about the products.

- **columns**:

| Column Name      | Data Type     | Description                                                                                              |
| :--------------- | :------------ | :------------------------------------------------------------------------------------------------------- |
| product_key      | INT           | Surrogate key identifying the product dimension.                                        |
| product_id       | INT           | Unique identifier assigned to each product row.                                    |
| product_number   | VARCHAR(50)   | Product number in the company system.                                                 |
| category_id      | INT           | Identifier for the product category.                                                      |
| category         | VARCHAR(50)  | Product category name. Example: "Mountain"                                                            |
| subcategory      | VARCHAR(50)  | Product subcategory name. Example: "Bikes"                                                          |
| maintenance      | VARCHAR(50)   | Maintenance details for the product. Example: "Annual checkup"                                            |
| product_cost     | INT | Cost of the product. Example: 199                                                                 |
| product_line     | VARCHAR(50)   | Product line or brand. Example: "Road"                                                                |
| start_date       | DATE          | Date when the product was introduced. Example: "2022-03-10"                                              |




### 3. gold.fact_sales

- **Purpose**: Stores sales transaction data.

- **columns**:

| Column Name     | Data Type     | Description                                                                                                        |
| :-------------- | :------------ | :----------------------------------------------------------------------------------------------------------------- |
| order_number    | VARCHAR(50)   | Unique identifier for the sales order.                                                |
| product_key     | INT           | Foreign key referencing the product dimension (gold.dim_products). Example: 67890                                  |
| customer_key    | INT           | Foreign key referencing the customer dimension (gold.dim_customers). Example: 12345                                |
| order_date      | DATE          | Date when the order was placed. Example: "2023-10-27"                                                              |
| shipping_date   | DATE          | Date when the order was shipped. Example: "2023-10-29"                                                             |
| due_date        | DATE          | Date when the order payment is due. Example: "2023-11-15"                                                          |
| sales_amount    | INT | Total sales amount for the order line. Example: 250                                                             |
| quantity        | INT           | Quantity of products sold in the order line. Example: 2                                                             |
| price           | INT | Unit price of the product sold. Example: 125                                                                   |
