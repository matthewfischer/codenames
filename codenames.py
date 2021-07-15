#!/usr/local/bin/python3

import boto3
import sys
import json
import random
from collections import OrderedDict

#
# Define S3Objects class
#

class S3Objects:

    s3Client = ""
    records = []

    #
    # Constructor
    #

    def __init__(self):
        self.bucketName = "codenamesbucket"
        self.s3ObjKey = "codenames.csv"
        self.s3Client = boto3.client('s3')
        #print(self.s3Client.list_buckets())

    def filter_s3_data_with_header(self):

        sqlQuery = "SELECT codename,description FROM s3object s"
        try:
            response = self.s3Client.select_object_content(Bucket=self.bucketName,
                                                            Key=self.s3ObjKey,
                                                            ExpressionType='SQL',
                                                            Expression=sqlQuery,
                                                            InputSerialization = {'CSV': { 'FileHeaderInfo': 'USE','FieldDelimiter': ',','RecordDelimiter': "\n",'QuoteCharacter': '"','QuoteEscapeCharacter': '"'},'CompressionType': 'NONE'},
                                                            OutputSerialization = {'JSON': { 'RecordDelimiter': "\n" },},
                                                          )

            # Create empty dataframe
            records = []
            for event in response['Payload']:
                if 'Records' in event:
                    json_record = event['Records']['Payload'].decode('utf-8')
                    recs = json_record.split('\n')
                    for r in recs:
                         if r != "":
                            j = json.loads(r, object_pairs_hook=OrderedDict)
                            self.records.append(j)

        except Exception as ex:
            print(ex)
            sys.exit(1)

    def random_record(self):
        idx = random.randrange(len(self.records))
        return self.records[idx]


if __name__ == "__main__":
    s3Obj = S3Objects()
    records = s3Obj.filter_s3_data_with_header()
    print("%s" % s3Obj.random_record()['description'])
