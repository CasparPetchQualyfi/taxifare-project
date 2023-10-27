
# Yellow taxi company analytics project
## Qualyfi accelerator final project
The goal of this project is to ingest a very large dataset in the form of many CSVs which represent trip data for a taxi company, clean it and transform it into a star schema to be read in Power BI and made into a dashboard, using a pipeline in Azure Data Factory that calls notebooks from inside Azure Databricks.

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
