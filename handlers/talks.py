import asyncio
import logging

import luigi

import tasks

logger = logging.getLogger(__name__)


@asyncio.coroutine
async def talk_message_handler(msg):
    """ Talk message event handler for the evented data pipeline scheduler.

    Creates a subscription in to the broker listening for messages with subject
    'pipeline.talk'.

    When a valid message is received by the subscription it triggers a luigid
    'tasks.playlist.Talk' task passing the message payload as an argument.
    """
    subject = msg.subject
    reply = msg.reply
    payload = msg.data.decode()

    msg = "Received message subject:'{:s}' reply:'{:s}' payload:{:s}".format(subject, reply, payload)
    logger.info(msg)

    luigi.build([tasks.talks.Talk(youtube_url=payload), ], log_level="WARNING")
