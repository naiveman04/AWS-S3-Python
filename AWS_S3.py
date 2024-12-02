import boto3
import botocore
import random
import string
import datetime
import numpy as np


class S3Manager:

    # Constructor
    def __init__(self):
        # Create a boto3 resource of type S3
        self.s3 = boto3.resource('s3')
        # Create a client
        self.client = boto3.client('s3')

    '''
        List all the buckets of your region of choice

        Input:
            regions (String) -- [REQUIRED]

        Output:
            list: All the bucket names of the specified regions
    '''

    def list_buckets(self, region):
        bucketList = list()
        for idx, each in enumerate(self.s3.buckets.all()):
            bucketList.insert(idx, each._name)
        return bucketList

    '''
        Create buckets with random name for the given regions list with ACL private.

        Input:
            bucket_name (String) -- Name of the bucket. If none, create a name with a random string
            regions(list) -- [REQUIRED]
            repeat (Integer) -- Number of times you want to repeat the "regions" list(Default is 1)

        Output:
            None
    '''
    def create_buckets(self, bucket_name=None, regions=[], repeat=1):
        if len(regions) == 0:
            print "No region found, please give at least one region name to create bucket."
            return 0
        else:
            regions *= repeat
            for idx, each in enumerate(regions):
                try:
                    # If bucket name is not given, create a random name
                    bucket_name = bucket_name if bucket_name is not None else "".join([random.choice(string.letters) for i in xrange(15)]).lower()
                    kw_args = {
                        'Bucket': bucket_name,
                        'ACL': 'private',
                        'CreateBucketConfiguration': {
                            'LocationConstraint': each
                        }
                    }

                    # Create a bucket
                    bucket = self.client.create_bucket(**kw_args)
                    # Check if the bucket is created successfully of not.
                    if bucket['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print "Created new bucket {} at region {}".format(bucket_name, each)
                    else:
                        print "Can not create a new bucket of name {}".format(bucket_name)

                # Got exception during process.
                except botocore.exceptions.ClientError as e:
                    print "{}".format(e)


    '''
        Upload a file in S3 bucket.

        Input:
            filename (String) -- [REQUIRED]
            bucket (String) -- [REQUIRED]
            key (String) -- [REQUIRED]

        Output:
            Key: The return value. Key for success, False otherwise.
    '''
    def upload_file(self, filename, bucket, key):
        try:
            print "Uploading a new file with key: {}".format(key)
            self.s3.meta.client.upload_file(filename, bucket, key)
            print "Successfully uploaded a file"
            return key

        # Got exception during process.
        except botocore.exceptions.ClientError as e:
            print "Upload failed. Please try again!"
            print e
            return 0



    '''
        Delete a file in S3 bucket.

        Input:
            filename (String) -- [REQUIRED]
            bucket (String) -- [REQUIRED]
            key (String) -- [REQUIRED]

        Output:
            Boolean: The return value. True for success, False otherwise.
    '''
    def delete_file(self, bucket, key):
        try:
            print "Deleting {} from bucket {}".format(key, bucket)
            if bool(self.client.delete_object(Bucket=bucket, Key=key)):
                print "Successfully deleted a file"
                return 1
            else:
                print "You don't have a file with the key: {}. Please try again with a right key".format(key)
                return 0

        # Got exception during process.
        except botocore.exceptions.ClientError as e:
            print "Deletion failed. Please try again!"
            print e
            return 0

    '''
        Download a file from S3 bucket.

        Input:
            bucket (String) -- [REQUIRED]
            key (String) -- [REQUIRED]
            filename (String) -- [REQUIRED]

        Output:
            Boolean: The return value. True for success, False otherwise.
    '''
    def download_file(self, bucket, key, filename):
        try:
            print "Downloading file {} from bucket {}".format(key, bucket)
            self.s3.Bucket(bucket).download_file(key, filename)
            print "Successfully downloded file {}".format(filename)
            return 1
        except botocore.exceptions.ClientError as e:
            print "Download failed. Please try again!"
            print e
            return 0
