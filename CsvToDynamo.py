import boto3
import csv
import json
import argparse

'''
You need to have aws configured with access tokens prior to running this script (use aws configure)
'''

def batch_create(table, csv_file_name, column_names):
    ''' 
    Can Handle many puts at one time. Boto3 gives an example of 50, even though
    max batch size is 25, because batch writer is buffering and sending items behind the scenes.
    '''
    print('Beginning csv to dynamo import...')

    with table.batch_writer() as batch:
        with open(csv_file_name, newline='') as csv_file:
            reader = csv.reader(csv_file)

            # skip first row which we know is a header row
            next(reader)

            count = 0
            for row in reader:
                item = {}
                for column in range (0, len(column_names)):
                    item[column_names[column]] = row[column]
                batch.put_item(Item=item)
                count += 1

                if count % 100 == 0:
                    print('Inserted ' + str(count) + ' items...')

            csv_file.close()

    print('Finished importing data into dynamo...')


def validate(table, csv_file_name, partition_key_col_name, sort_key_col_name):
    print('Beginning data validation...')

    with open(csv_file_name, newline='') as csv_file:
        reader = csv.reader(csv_file)

        # skip first row which we know is a header row
        next(reader)

        for row in reader:
            key = {partition_key_col_name: row[0], sort_key_col_name: row[1]}

            try:
                response = table.get_item(Key=key)
                assert('Item' in response)
            except AssertionError:
                print('Failed to validate data. Key ' + json.dumps(key) + ' does not exist...')

        csv_file.close()

    print('Finished data validation...')

def main():
    csv_file_name = ''
    table_name = ''
    region = 'us-west-2'
    partition_key_col_name = ''
    sort_key_col_name=''
    column_names = [partition_key_col_name, sort_key_col_name, 'Column3']

    dynamodb_resource = boto3.resource('dynamodb', region_name=region)
    table = dynamodb_resource.Table(table_name)

    batch_create(table, csv_file_name, column_names)
    validate(table, csv_file_name, partition_key_col_name, sort_key_col_name)

if __name__ == "__main__":
    main()