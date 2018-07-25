import logging

import handlers

from scheduler import Scheduler

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logger.info('Starting the pipeline-scheduler ...')
    scheduler = Scheduler()
    scheduler.event_loop.run_until_complete(scheduler.boostrap())

    logger.info('Setup event subscriptions and message handlers ...')
    scheduler.subscribe("pipeline.channel", handlers.channels.channel_message_handler)
    scheduler.subscribe("pipeline.playlist", handlers.playlists.playlist_message_handler)
    scheduler.subscribe("pipeline.talk", handlers.talks.talk_message_handler)

    logger.info("Setup event dispatchers and message publishers ...")

    try:
        scheduler.event_loop.run_forever()
    finally:
        scheduler.event_loop.close()