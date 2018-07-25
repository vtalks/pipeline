import logging
import asyncio
import signal

from nats.aio.client import Client as NATS

logger = logging.getLogger(__name__)


class Scheduler:
    """ Scheduler instance
    """
    # The event loop
    event_loop = None

    # NATS client instance
    nats_client = None

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()
        self.nats_client = NATS()

    def signal_handler(self):
        """ Shutdown scheduler client gracefully
        """
        if self.nats_client.is_closed:
            return
        logger.info("Closing NATS connection ...")
        self.event_loop.create_task(self.nats_client.close())

    @asyncio.coroutine
    def closed_callback(self):
        """ Callback to close NATS clientvconnection.
        """
        yield from asyncio.sleep(0.1, loop=self.event_loop)
        self.event_loop.stop()

    @asyncio.coroutine
    def reconnected_callback(self):
        """ Callback for NATS clientvreconnections.
        """
        logger.info("Connected to NATS at {}...".format(self.nats_client.connected_url.netloc))

    async def connect_NATS(self, options):
        """ Connect to NATS server.
        """
        await self.nats_client.connect(**options)

    async def boostrap(self):
        """ Create a NATS client and listen for signals to graceful shutdown
        the scheduler.
        """
        options = {
            "servers": ["nats://nats:4222"],
            "io_loop": self.event_loop,
            "closed_cb": self.closed_callback,
            "reconnected_cb": self.reconnected_callback,
        }
        await self.connect_NATS(options)

        # Listen for signals to graceful shutdown
        for sig in ('SIGINT', 'SIGTERM'):
            self.event_loop.add_signal_handler(getattr(signal, sig), self.signal_handler)

    def subscribe(self, subject, callback):
        """ Subscribe to message event and assign a callback to execute.
        """
        self.event_loop.create_task(self.nats_client.subscribe(subject, cb=callback))
        msg = "Subscribe to {:s}".format(subject)
        logger.info(msg)