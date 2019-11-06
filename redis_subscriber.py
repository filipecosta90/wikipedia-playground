#!/usr/bin/env python
"""Module and script to parse pub-sub events and write them to Redis hashes."""
import asyncio as aio

import aioredis

from wikiutils import current_timestamp, pubsub_channel, redis_url


async def process():
    redis = await aioredis.create_redis_pool(redis_url)
    while True:

        try:
            """Process messages from the stream."""
            result = await redis.xread([pubsub_channel])
            await process_message(redis, result)

        except (ConnectionError, aio.TimeoutError, aio.CancelledError):
            pass


async def process_message(redis, message):
    """Process single decoded message."""

    stream_ts = int(message[0][1].decode('ascii').split("-")[0]) / 1000
    ts = current_timestamp(stream_ts)
    message = dict(message[0][2])
    if b'meta_domain' in message.keys():
        domain = message[b'meta_domain'].decode('ascii')
        keys = ["ev", f"ev:{domain}"]
        pipe = redis.pipeline()
        for key in keys:
            pipe.hincrby(key, ts, 1)
        pipe.sadd("known_domains", domain)
        await pipe.execute()


if __name__ == "__main__":
    aio.run(process(), debug=True)
