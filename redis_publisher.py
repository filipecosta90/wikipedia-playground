#!/usr/bin/env python
"""Module and script to forward events from wikimedia stream to Redis pub-sub."""
import asyncio as aio
import json

import aioredis
from aiohttp_sse_client import client as sse_client

from wikiutils import pubsub_channel, redis_url

wikimedia_stream = "https://stream.wikimedia.org/v2/stream/recentchange"


def flatten_json(nested_json):
    """
        Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        elif type(x) is bool:
            out[name[:-1]] = str(x)
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


async def publish():
    """Read events from pub-sub channel."""
    redis = await aioredis.create_redis_pool(redis_url)
    while True:
        async with sse_client.EventSource(wikimedia_stream) as event_source:
            try:
                async for event in event_source:
                    await redis.xadd(pubsub_channel, flatten_json(json.loads(event.data)))
            except (ConnectionError, aio.TimeoutError, aio.CancelledError):
                pass


if __name__ == "__main__":
    aio.run(publish(), debug=True)
