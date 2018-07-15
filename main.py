import logging
import asyncio
import signal

from handlers import talks
from handlers import channels
from handlers import playlists

from nats.aio.client import Client as NATS

logger = logging.getLogger('root')

logging.getLogger('sh').setLevel(level=logging.WARNING)
logging.getLogger('luigi-interface').setLevel(level=logging.WARNING)


async def run(loop):
    """ Asynchronous main entry point for the scheduler
    """

    @asyncio.coroutine
    def closed_cb():
        """ Callback to close NATS client connection
        """
        logger.info("Connection to NATS is closed.")
        yield from asyncio.sleep(0.1, loop=loop)
        loop.stop()

    # Connect to NATS
    nc = NATS()
    options = {
        "servers": ["nats://nats:4222"],
        "io_loop": loop,
        "closed_cb": closed_cb,
    }
    await nc.connect(**options)
    logger.debug("Connected to NATS")

    @asyncio.coroutine
    async def pipeline_message_handler(msg):
        """ General message handler for the data pipeline scheduler
        """
        subject = msg.subject
        reply = msg.reply
        payload = msg.data.decode()

        msg = "Received a message subject:'{:s}' reply:'{:s}' payload:{:s}".format(subject, reply, payload)
        logger.info(msg)

        if subject == "pipeline.talk":
            msg = "Handling subject:'{:s}'".format(subject)
            logger.info(msg)
            talks.handle(payload)
        elif subject == "pipeline.channel":
            msg = "Handling subject:'{:s}'".format(subject)
            logger.info(msg)
            channels.handle(payload)
        elif subject == "pipeline.playlist":
            msg = "Handling subject:'{:s}'".format(subject)
            logger.info(msg)
            playlists.handle(payload)
        else:
            msg = "Unknown message subject:'{:s}, message won't be processed".format(subject)
            logger.warning(msg)

    # Subscription
    await nc.subscribe("pipeline.*", cb=pipeline_message_handler)
    logger.debug("Subscribed to pipeline.* messages")

    # Shutdown scheduler gracefully
    def signal_handler():
        if nc.is_closed:
            return
        logger.debug("Disconnecting...")
        loop.create_task(nc.close())

    # Listen for signals to graceful shutdown
    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info('Starting pipeline-scheduler ...')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
