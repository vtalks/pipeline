import sys
import asyncio
import signal

from nats.aio.client import Client as NATS


async def run(loop):
    nc = NATS()

    @asyncio.coroutine
    def closed_cb():
        print("Connection to NATS is closed.")
        yield from asyncio.sleep(0.1, loop=loop)
        loop.stop()

    options = {
        "servers": ["nats://nats:4222"],
        "io_loop": loop,
        "closed_cb": closed_cb,
    }
    await nc.connect(**options)

    @asyncio.coroutine
    async def pipeline_message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on subject:'{subject}' reply:'{reply}': {data}".format(subject=subject, reply=reply, data=data))

    await nc.subscribe("pipeline.*", cb=pipeline_message_handler)

    def signal_handler():
        if nc.is_closed:
            return
        print("Disconnecting...")
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)


def main(argv):
    print('Starting pipeline-scheduler ...')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main(sys.argv[1:])