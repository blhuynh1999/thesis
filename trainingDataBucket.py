import botocore.exceptions

from utilities.log import Logger
from utilities.config import Config
from utilities.notifier import Notifier

L = Logger()
C = Config.getInstance()
N = Notifier.getInstance()


class TrainingDataBucket(object):

    def __init__(self, client):

        self.client = client

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def createBucket(self):

        bucketName = 'i-{0}-{1}-{2}'.format(C.items['exchange'].lower(
        ), C.items['coin'].lower(), C.items['ingesterId'].lower())

        try:
            response = self.client.create_bucket(
            ACL='private', Bucket=bucketName)

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise Exception(e)

            response = e

        finally:
            C.items['trainingDataBucket']['name'] = bucketName
            return response

    @Logger.logTrace(L)
    @N.notifyAndExitIfException
    def uploadFile(self, path, key):

        self.client.upload_file(
            path, C.items['trainingDataBucket']['name'], key)