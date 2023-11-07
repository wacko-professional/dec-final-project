# dec-final-project

## Project Plan

### Objective
Positioning ourselves as a Brazilian E-Commerce player (Olist), the objective of this project is to automate the creation of data/tables that will help analysts, data scientists and management of the company to make better business decisions and track KPIs.

This will be done by potentially:
- Creating the neccessarry fact tables pertaining to sales, order status.
- Creating the neccessarry dimensional tables pertaining to customers, products and reviews.
- Applying dimensional modelling techniques to track inventory, lead times, price histories of products as well as monthly aggregations (to be able to track the growth of the business).
- Allowing the visualisation of the aforementioned data/tables through a dashboard and running batch jobs at a daily frequency.


### Consumers
The envisioned end consumers are the analysts, data scientists and management (e.g. PMO office) of Olist, who would utilise the data to e.g. initiate continuous improvement projects, make informed business decisions based on observed growth, run pricing experiments, etc.

### Dataset
The dataset has been obtained from kaggle:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_order_items_dataset.csv

The following source has been used to migrate the relevant data/tables to a psql database:
https://github.com/pauloreis-ds/olist-database-postgresql


### Solution Architecture
The proposed solution architecture is as shown by the following diagram:

![Alt text](solution_architecture.png)


### Breakdown of Tasks
The project is broken down according to the following tasks:
- Setting up a project repository.
- Create dimensional models.
- Creating an RDS instance for the Olist database and migrating data to populate it.
- Hosting Airbyte on an EC2 instance.
- Configuring extract and load batch data integrations steps using Airbyte for ingestion into Databricks Lakehouse.
- Creating databricks notebooks (bronze, silver and gold layers) for batch transform steps.
- Creating tests using Great Expectations in the transform notebooks.
- Orchestrating the notebooks using Databricks workflow.
- Create a dashboard using databricks SQL dashboards to visualise the relevant metrics/data from the fact and dim tables.
- Configuring CI/CD using Github actions for deploying to a Databricks repo.
