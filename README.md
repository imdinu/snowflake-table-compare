# Compare snowflake tables

This simple script downloads and compares snowflake tables by counting columns and rows and matching column values based on row unique IDs.

## Setting up

You just need to have **python 3+** installed and then install the required libraries using:
```sh
$ pip install -r requirements.txt
```

Lastly, change the content of [default.credentials.json](default.credentials.json) to reflect your Snowflake login information 

## Running the comparison

From the terminal do:
```sh
$ python compare.py <reference table> <target table> --key <primary key>
```

There are additional options for comparison that you may consult using 
```sh
python compare.py --help
```

## Bulk comparison

Having prepared a csv file with columns for source target and primary key information, run:
```sh
$ python bulk_compare.py <tables csv file> 
```
