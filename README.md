
# Yellow taxi company analytics project
## Qualyfi accelerator final project
The goal of this project is to ingest a very large dataset in the form of many `.CSV` files which represent trip data for a taxi company, clean it and transform it into a star schema to be read in Power BI and made into a dashboard, using a pipeline in Azure Data Factory that calls notebooks from inside Azure Databricks.

# 1. Data modelling

The conceptual, logical and physical models were created first using the website draw.io. They were designed in order to answer the following questions about the data:  
* Which vendor is charging the lowest fare per distance traveled? 

* What are the most common pick up and drop off locations? 

* Which vendor is ranked the highest (create a ranking metric)? 

* What are the worst and best times to get a taxi? 

* Any other questions that can be answered that are useful for stakeholders & users


I mostly chose columns for data that already existed in the dataset, and included some extra columns that data would have to be collected (for this example, randomly generated) in order to have a dataset that can answer the posed questions. The extra included columns were `Waiting_time_minutes` and `Customer_review_score` which would provide me a metric to rank the vendors.

The model diagrams can be seen below:
## Conceptual model
![image](conceptual.png)

## Logical model
![image](logical.png)

## Physical model
![image](physical.png)

# 2. Power BI wireframe
I created visualisations that will allow users to quickly understand and assess the data in order to answer the stated questions. They are filled with dummy data at this stage. The dashboard is split into three pages as shown below:

## Vendor comparison
![image](vendor_powerbi.png)

## Map
![image](map_powerbi.png)

## Times of day
![image](times_powerbi.png)

# 3. Infrastructure as code
I used Terraform to create the containers I would be working in, namely `landing-cp`, `bronze-cp`, `silver-cp`, `gold-cp`. These represent the progression of the data manipulation. Landing is simply a direct copy of the input data, 132 large `.CSV` files. Bronze, silver and gold are further and further processed versions of that data, to be eventually fed into Power BI.

# 4. Data engineering
In Azure Databricks, I created new column data and partitioned the data in bronze, applied a schema and some filters in silver, and reformatted the data into a star schema with primary and foreign keys for joining in gold. I created a pipeline which would clear all four containers, copy the source data into landing, and finally run the data processing steps to end up with the final dataset in gold.

![image](pipe.png)
