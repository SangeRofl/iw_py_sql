# Program
## Purpose
The program reads data from files specified by the user, loads them into the database, generates 4 queries and uploads the result to XML or JSON format.
## Usage example
This code creates 4 xml files with the queries results in output/ directory

```python .\main.py -f xml .\data\students.json .\data\rooms.json```

## Run test
```python .\test.py```

## Used indexes
To optimize queries, a B-tree index was created on the birthday column of the students table.