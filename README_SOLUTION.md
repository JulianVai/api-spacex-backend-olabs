# Solution by Julián F. Vega D.

A basic class was implemented using pandas and sqlalchemy connector to interact with a mysql database.

## Part 1
To implement the solution is important to have the mysql database running for that execute:

```bash
docker compose -f docker-compose.yaml up
```

## Testing the features.

The implementation of features was compiled into an `SpaceXUtil` class.

To instanciate and execute features:
```python
from space_x_utility import SpaceXUtil
# Use the name of the input file ton instantiate  the class
sx_util = SpaceXUtil("starlink_historical_data.json")

# Obtain a copy for inspection of the cleaned and transformed the relevant data.
r_df = sx_util.transform_clean_raw()
```
## Part 2
To load the data into the mysql database 
```python
sx_util.load_to_mysql()
```
This will load a version of the already transformed and cleaned data.


## Part 3
To ask for the last known position of a satelite using its ID use:
```python
sx_util.last_known_position("5eed7714096e590006985643")
>>>{'creation_date': datetime.datetime(2021, 1, 26, 6, 26, 10),
 'longitude': 115.0,
 'latitude': 14.3871,
 'id': '5eed7714096e590006985643',
 'object_name': 'STARLINK-48',
 'object_id': '2019-029BG'}
```
Under the hood this is implementing a query that initially calls for all the last known position of all the satellites and then filters the desired record by the id of interest. 

## Part 4
By implementing
```python
sx_util.closest_sat(ref_lat=45.75, ref_long=4.84,time_s="2021-01-28 06:26:10")
>>> '5f5a9c1e2fd30c00065e5ec6'
```
Here its important that the ISO format of the timestamp is used.

