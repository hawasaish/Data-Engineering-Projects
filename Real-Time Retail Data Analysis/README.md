# Real-Time Retail Data Analysis

In this project, we will broadly perform the following tasks:

- Reading the sales data from the Kafka server.

- Preprocessing the data to calculate additional derived columns such as total_cost etc.

- Calculating the time-based KPIs and time and country-based KPIs.

- Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis.



We will be calculating four KPIs on a tumbling window of one minute on orders across the globe. We will also be calculating the first three KPIs on a per-country basis.

The KPIs that we will be calculating are as follows:

- Total volume of sales:

By calculating this, we can see where and when demand is higher and try to understand the reason. It helps in creating projections for the future. The total volume of sales is the sum of the transaction value of all orders in the given time window. The total transaction value of a specific order will be calculated as follows:

∑(quantity ∗ unit price)

The total volume of sales in a time interval can be calculated as the summation of the transaction values of all the orders in that time interval. Also, the sales data stream contains a few returns. In the case of returns, the transaction amount needs to be subtracted. So, the equation for the total volume of sales, in this case, will be calculated as follows:

∑ Order(quantity * unit price) − ∑ Return (quantity ∗ unit price)


- OPM (orders per minute):

Orders per minute (OPM) is another important metric for e-commerce companies. It is a direct indicator of the success of the company. As the name suggests, it is the total number of orders received in a minute.


- Rate of return: 

No business likes to see a customer returning their items. Returns are costly because they need to be processed again and have adverse effects on revenue. The rate of return indicates customer satisfaction for the company. The more satisfied the customer is, the lower the rate of return will be. The rate of return is calculated against the total number of invoices using the following equation:

∑ Returns / (∑ Returns + ∑ Orders)


- Average transaction size:

The average transaction size helps in measuring the amount of money spent on average for each transaction. Evaluating this KPI over a year is a good indicator of when the customers are more likely to spend money, which enables the company to adapt their advertising accordingly. This can be calculated using the following equation:

Total Sales Volume = ∑Returns +∑ Orders


We will be calculating the four KPIs mentioned above for all the orders. We will also be calculating the first three KPIs on a per-country basis.

Note that in some cases, the total sales volume can be negative if the return cost is more than that of new orders in that window.



### The tasks that we will be performing in this project are as follows

- Reading input data from Kafka 

Code to take raw Stream data from Kafka server

Details of the Kafka broker are as follows (Server details cannot be made public due to privacy & security reasons):

Bootstrap Server - ##.###.###.###

Port - ####

Topic - real-time-project

- Calculating additional columns and writing the summarised input table to the console

The following attributes from the raw Stream data have to be taken into account for the project:

1. invoice_no: Identifier of the invoice

2. country: Country where the order is placed

3. timestamp: Time at which the order is placed

In addition to these attributes, the following UDFs have to be calculated and added to the table:

1. total_cost: Total cost of an order arrived at by summing up the cost of all products in that invoice (The return cost is treated as a loss. Thus, for return orders, this value will be negative.)

2. total_items: Total number of items present in an order

3. is_order: This flag denotes whether an order is a new order or not. If this invoice is for a return order, the value should be 0.

4. is_return: This flag denotes whether an order is a return order or not. If this invoice is for a new sales order, the value should be 0.

- The input table must be generated for each one-minute window.

Code to define the schema of a single order

Code to define the aforementioned UDFs and any utility functions are written to calculate them

Code to write the final summarised input values to the console. This summarised input table has to be stored in a Console-output file.


### Calculating time-based KPIs:

- Code to calculate time-based KPIs tumbling window of one minute on orders across the globe. These KPIs were discussed in the previous segment.

- KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.


### Calculating time- and country-based KPIs:

- Code to calculate time- and country-based KPIs tumbling window of one minute on orders on a per-country basis. These KPIs were discussed in the previous segment.

- KPIs have to be calculated for a 10-minute interval for evaluation; so, ten 1-minute window batches have to be taken.


### Writing all the KPIs to JSON files:

- Code to write the KPIs calculated above into JSON files for each one-minute window.