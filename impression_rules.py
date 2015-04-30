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
        assert params.ip_pattern.match(parts['ip'])
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

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 0, 'enhanced': False}
    view = parts.get('view', sys.maxint)

    try:

        # now prepare values for emitting this particular event
        if parts.get('click') is not None:
            position = parts['click']
            vals['clicks'] = 1
            tiles = [tiles[position]]
        elif parts.get('pin') is not None:
            position = parts['pin']
            vals['pinned'] = 1
            tiles = [tiles[position]]
        elif parts.get('block') is not None:
            position = parts['block']
            vals['blocked'] = 1
            tiles = [tiles[position]]
        elif parts.get('sponsored') is not None:
            position = parts['sponsored']
            vals['sponsored'] = 1
            tiles = [tiles[position]]
        elif parts.get('sponsored_link') is not None:
            position = parts['sponsored_link']
            vals['sponsored_link'] = 1
            tiles = [tiles[position]]
        else:
            vals['impressions'] = 1
            cparts = parts.copy()
            del cparts['tiles']
            cparts['newtabs'] = 1
            yield cparts

        del parts['tiles']

        # emit all relavant tiles for this action
        for i, tile in enumerate(tiles):
            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts.update(vals)

            # the position can be specified implicity or explicity
            if tile.get('pos') is not None:
                slot = tile['pos']
            elif position is None:
                slot = i
            else:
                slot = position
            assert position < 1024
            cparts['position'] = slot

            url = tile.get('url')
            if url:
                cparts['enhanced'] = True
                cparts['url'] = url

            tile_id = tile.get('id')
            if tile_id is not None and isinstance(tile_id, int) and tile_id < 1000000 and slot <= view:
                cparts['tile_id'] = tile_id
            yield cparts
    except:
        print "Error parsing tiles: %s" % str(tiles)


def filter_x(parts, params, **kwargs):
    if parts['tile_id'] == 504 and parts['clicks'] > 0:
        yield parts


def filter_clicks(keys, vals, params, threshold=1):
    if vals[0] > threshold:
        yield keys, vals

RULES = [
    InfernoRule(
        name='ip_click_counter',
        source_tags=['incoming:impression'],
        map_input_stream=chunk_json_stream,
        parts_preprocess=[clean_data, parse_tiles, filter_x, count],
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['ip'],
        value_parts=['count'],
        parts_postprocess=[partial(filter_clicks, threshold=1)],
    ),
]
