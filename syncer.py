import boto3
import datetime

from twisted.internet import reactor

if __name__ == '__main__':

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

    from data.syncerSettingsDB import SyncerSettingsDB
    dynamodb = boto3.resource('dynamodb')
    syncerSettingsDB = SyncerSettingsDB(dynamodb)
    settings = syncerSettingsDB.getSettings()
    C.items = {**settings, **C.items}

    from utilities.startup import Startup
    Startup.setup()

    from data.ingesterBotsDB import IngesterBotsDB
    ingesterBotsDB = IngesterBotsDB(dynamodb)

    from data.trainingDataBucket import TrainingDataBucket
    boto3Session = boto3.session.Session()
    client = boto3Session.client('s3',
        region_name=C.items['trainingDataBucket']['region'],
        endpoint_url=C.items['trainingDataBucket']['endpoint'],
        aws_access_key_id=C.items['trainingDataBucket']['key'],

        aws_secret_access_key=C.items['trainingDataBucket']['secret'])
    trainingDataBucket = TrainingDataBucket(client)

    from downloadManager import DownloadManager
    downloadManager = DownloadManager(ingesterBotsDB, trainingDataBucket)
    downloadManager.run()

    reactor.run()