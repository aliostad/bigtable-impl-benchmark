import data_builder
import boto


ddb = boto.connect_dynamodb()
i = ddb.get_item("", "")