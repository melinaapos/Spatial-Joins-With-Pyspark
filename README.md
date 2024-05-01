# About the project

The objective of this project is to identify pairs of hotels and restaurants situated within a distance of 500 meters from each other, utilizing the available datasets containing information about hotels and restaurants. The project is implemented in **Pyspark** (the Python API for Apache Spark).

### Haversine Distance

The distance utilized for calculating the distance between a hotel and a restaurant is determined using the *Haversine* formula. The Haversine formula calculates the distance between two points on Earth using their longitude and latitude.

### Implementation

Given the extensive volume of data in both datasets and the computational expense of computing haversine distances, calculating the distance between every pair of hotels and restaurants proves inefficient. Hence, this project employs a strategy to minimize the number of such calculations required. To achieve this, both datasets are sorted by the *latitude* column and then repartitioned using `repartitionByRange` based on the same *latitude* column. This ensures that data entries with similar latitude values are more likely to reside within the same partition. Then, the two datasets undergo a full outer join operation on the integer value of latitude. Within the resulting joined dataframe, haversine distances are computed, followed by a filtering process to retain only those pairs with distances less than 0.5 units, effectively representing locations within 500 meters of each other. As a result, the output is a dataframe containing pairs of hotels and restaurants situated within a 500-meter proximity to one another.


## Environment

The selected environment for this project is Google Colab which is a hosted Jupyter Notebook service that requires no setup to use and provides free access to computing resources. In addition to the installations for pyspark, haversine (the distance which is used) was installed via *pip* with the following command:

`pip install haversine`

## Datasets

For this implementation, two datasets were employed: one contaning information about hotels and the other containing information about restaurants. The utilized columns from the datasets are as follows:

Restaurants Dataset:

+ Restaurant_Id
+ Restaurant_name
+ Restaurant_latitude
+ Restaurant_longitude

Hotels Dataset:

+ Hotel_Id
+ Hotel_name
+ Hotel_latitude
+ Hotel_longitude

