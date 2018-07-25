import logging
import asyncio
import signal

import handlers

from nats.aio.client import Client as NATS

logger = logging.getLogger(__name__)


async def run(loop):
    """ Asynchronous main entry point for the scheduler
    """

    @asyncio.coroutine
    def closed_cb():
        """ Callback to close NATS client connection
        """
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

    # Subscriptions
    await nc.subscribe("pipeline.channel", cb=handlers.channels.pipeline_channel_message_handler)
    # await nc.subscribe("pipeline.playlist", cb=handlers.playlists.pipeline_playlist_message_handler)
    await nc.subscribe("pipeline.talk", cb=handlers.talks.pipeline_talk_message_handler)

    # Shutdown scheduler gracefully
    def signal_handler():
        if nc.is_closed:
            return
        loop.create_task(nc.close())

    # Listen for signals to graceful shutdown
    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger.info('Starting pipeline-scheduler ...')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
