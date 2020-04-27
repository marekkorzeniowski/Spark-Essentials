# Spark-Essentials
Basic spark operations with a small application at the end allowing digesting big amounts of data for a taxi company (app. 35 GB).

This repo was created based on the course provided by RockTheJVM. Its aim is to demonstrate skills and knowledge gained during its participation.

Mentioned above application was tested on a local machine, as well as, on AWS EMR cluster to compare performance and to get a taste of cloud computing.




-----------
## Some thougths and assumptions about carried-out analysis

### Objectives
- Gathering meaningful insight from a data
- improve company performance 
- maximalize income
- search for the fields of possible improvement and savings

### Observations and proposals
1) Manhattan the most popular area of pickup and dropoff.
Differentiate prices according to the pickup/dropoff area, and by demand.

2) Peak hours between 6pm and 10pm and a significant jump in demand between 5 pm and 6 pm approximately 20% can suggest differentiation of prices according to demand.

3) There is a clear distinction between long and short trips. Short trips predominate in wealthy areas like Manhattan and long trips are mainly transfers between airports. For the management team of taxi-company, it’s worth considering the separation of the market and tailoring services for each.

4)  Credit cards are the most common payment methods. There is two orders of magnitude between cash and credit cards. Taxi drivers should provide its availability 24/7, otherwise, the company can get in trouble with payments.

5) There are lots of close taxi rides within 5 min time span, therefore ride-sharing is worth exploring.

### Possible benefits:
- discount for people who take a grouped ride, which can significantly lower CO2 emission as well as fuel savings for the taxi company
- overall lowering cost of its service can leverage competitivity of the company among other competitors on the market


### Economical impact
For the sake of the possible economic impact some assumptions where done.
- 5% of taxi trips detected to be groupable at any time
- 30 % of people actually accept to be grouped
- $5 discount if you take a grouped ride
- $2 extra to take an individual ride (privacy/ time)
- if two rides grouped, reducing cost by 60% of one average ride

### Results
Possible $ 40k of additional income per day coming from savings and other profits resulting from car-sharing. Presented results multiplied by a number of days in a year can yield even 1.4 billion per year.
