# csv-to-dynamodb
Imports a csv file into a dynamo db and validates that all entries were created. CSV File is assumed to have a header line. For example, 

PartionKey, Column1, Column2
'ab-12-cd', 'hello world', 5

Run `pip install boto3` and `aws configure` and tweak the values for csv file path, table name, etc.