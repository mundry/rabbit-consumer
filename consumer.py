from __future__ import annotations

import logging.config
import os
import sys
from time import sleep
from typing import TYPE_CHECKING

import pika
import yaml
from pika.exceptions import ChannelClosed

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


def setup_logging(configfile="logging.yaml") -> None:
    with open(configfile) as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)


logger = logging.getLogger("consumer")


def get_env(name) -> str:
    value = os.environ.get(name, "").strip()

    if len(value) == 0:
        logger.error("Value for '%s' is empty.", name)
        sys.exit(1)

    return value


def callback(ch, method, properties, body) -> None:
    payload = body.decode("utf-8").strip()
    logger.info("[x] Received %r from queue", payload)
    title, sleeptime = payload.split(";", 1)

    logger.info("[x] Received %r", title)

    logger.info("[x] Sleeping for %s seconds to simulate processing.", sleeptime)
    sleep(int(sleeptime))

    logger.info("[x] Finished processing %r. Sending acknowledgement.", title)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    logger.info(
        "[x] Acknowledgment sent for %r (%r). Exiting callback.",
        title,
        method.delivery_tag,
    )


def setup_consumer() -> tuple[BlockingChannel, pika.BlockingConnection]:
    host = get_env("RABBIT_HOST")
    user = get_env("RABBIT_USER")
    password = get_env("RABBIT_PASS")
    queue = get_env("RABBIT_QUEUE")
    heartbeat = int(get_env("RABBIT_HEARTBEAT"))
    timeout = int(get_env("RABBIT_TIMEOUT"))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=host,
            credentials=pika.PlainCredentials(user, password),
            heartbeat=heartbeat,
            blocked_connection_timeout=timeout,
        ),
    )
    channel = connection.channel()

    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)

    logger.info(
        "[x] Sending consume command to broker. Listening for messages on RABBIT.",
    )
    channel.basic_consume(queue=queue, auto_ack=False, on_message_callback=callback)

    return channel, connection


if __name__ == "__main__":
    setup_logging()
    channel, connection = setup_consumer()

    while True:
        try:
            logger.info("[x] Waiting for messages on RABBIT.")
            channel.start_consuming()
            logger.info("[x] Stopped consuming. Exiting.")
        except KeyboardInterrupt:
            logger.info(
                "[x] Received interrupt. Stopping consuming messages and closing connection.",
            )
            channel.stop_consuming()
        except ChannelClosed:
            logger.exception("[x] Channel closed.")
        except:
            logger.exception("[x] Unexpected error.")
        finally:
            connection.close()

        sleep(3)
