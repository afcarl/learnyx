from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
import logging
from functools import partial

log = logging.getLogger(__name__)


def combiner(key, value, buf, done, params):
    if not done:
        i = len(value)
        buf[key] = [a + b for a, b in zip(buf.get(key, [0] * i), value)]
    else:
        return buf.iteritems()


def clean_data(parts, params, imps=True):
    import datetime
    try:
        if imps:
            assert parts['tiles'][0] is not None
        assert datetime.datetime.fromtimestamp(parts['timestamp'] / 1000.0)
        parts['locale'] = parts['locale'][:12]
        if parts.get('action'):
            parts['action'] = parts['action'][:254]
        yield parts
    except:
        pass


def count(parts, params):
    parts['count'] = 1
    yield parts


def parse_tiles(parts, params):
    import sys
    from urlparse import urlparse
    """If we have a 'click', 'block' or 'pin' action, just emit one record,
        otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    try:

        # now prepare values for emitting this particular event
        if parts.get('click') is not None:
            position = parts['click']
            tile = tiles[position]

            del parts['tiles']

            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts['clicks'] = 1

            tile_id = tile.get('id')
            if tile_id is not None and isinstance(tile_id, int) and tile_id < 1000000:
                cparts['tile_id'] = tile_id
                yield cparts
    except:
        print "Error parsing tiles: %s" % str(tiles)


def filter_all(parts, params, **kwargs):
    for col, val in kwargs.items():
        if col and parts[col] != val:
            return
    yield parts


def filter_clicks(keys, vals, params, threshold=1):
    if vals[0] > threshold:
        yield keys, vals

RULES = [
    InfernoRule(
        name='ip_click_counter',
        source_tags=['incoming:impression'],
        map_input_stream=chunk_json_stream,
        parts_preprocess=[clean_data, parse_tiles, partial(filter_all, tile_id=504), count],
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['ip'],
        value_parts=['count'],
        parts_postprocess=[partial(filter_clicks, threshold=5)],
    ),
]
