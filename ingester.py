import copy
import json
import boto3
import datetime

from twisted.internet import reactor

def waitTillIngesterJob():

    from utilities.config import Config
    C = Config.getInstance()

    from data.ingesterJobsQueue import IngestersJobsQueue
    sqs = boto3.client('sqs')
    ingestersJobsQueue = IngestersJobsQueue(sqs)

    job, reciept = ingestersJobsQueue.getJob()

    if job != None:

        C.items = {**job, **C.items}

        from data.ingesterSettingsDB import IngesterSettingsDB
        dynamodb = boto3.resource('dynamodb')
        ingesterSettingsDB = IngesterSettingsDB(dynamodb)
        settings = ingesterSettingsDB.getSettings()
        C.items = {**settings, **C.items}

        from data.exchangesDB import ExchangesDB
        exchangesDB = ExchangesDB(dynamodb)
        exchangeInfo = exchangesDB.getExchangeInfo()
        C.items['exchangeInfo'] = copy.deepcopy(exchangeInfo)

        from data.syncJobsQueue import SyncJobsQueue
        syncJobsQueue = SyncJobsQueue(sqs)
        syncJobsQueue.createQueue()

        from data.ingesterBotsDB import IngesterBotsDB
        ingesterBotsDB = IngesterBotsDB(dynamodb)
        record = copy.deepcopy(job)
        record['startTs'] = C.items['startTs']
        record['status'] = 'started'
        record['sequence'] = json.dumps({})
        record['syncQueueUrl'] = C.items['syncJobQueueSettings']['url']

        ingesterBotsDB.createRecord(record)

        from data.trainingDataBucket import TrainingDataBucket
        boto3Session = boto3.session.Session()
        client = boto3Session.client('s3',
            region_name=C.items['trainingDataBucket']['region'],
            endpoint_url=C.items['trainingDataBucket']['endpoint'],
            aws_access_key_id=C.items['trainingDataBucket']['key'],
            aws_secret_access_key=C.items['trainingDataBucket']['secret'])
        trainingDataBucket = TrainingDataBucket(client)
        trainingDataBucket.createBucket()

        from utilities.startup import Startup
        S = Startup()
        S.setup()

        ingestersJobsQueue.acceptJob(reciept)
        ingesterBotsDB.updateStatus('running')

        from batchManager import BatchManager
        batchManager = BatchManager(
            syncJobsQueue, ingesterBotsDB, trainingDataBucket)

    else:

        reactor.callLater(C.items['ingesterJobsQueue']
            ['pollDelay'], waitTillIngesterJob)

if __name__ == "__main__":

    datetimeNow = datetime.datetime.now()
    startTs = datetimeNow.strftime("%Y-%m-%d-%H-%M-%S")

    from utilities.config import Config
    C = Config.getInstance()
    C.items['startTs'] = startTs
    from utilities.log import Logger
    Logger.setup()
    logger = Logger()
    logger.debug('Logger setup completed')

    from utilities.notifier import Notifier
    from data.notificationEmail import NotificationEmail
    ses = boto3.client('ses')
    notificationEmail = NotificationEmail(ses)
    notifier = Notifier(notificationEmail)
    reactor.callLater(C.items['ingesterJobsQueue']
        ['pollDelay'], waitTillIngesterJob)
    reactor.run()