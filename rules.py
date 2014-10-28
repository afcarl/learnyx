from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_postgres, insert_redshift
from infernyx.rules import impression_stats_init, parse_date, parse_ip, parse_ua, combiner
from functools import partial
from config_infernyx import *

AUTO_RUN = False


def count(parts, params):
    parts['count'] = 1
    yield parts


def parse_tiles(parts, params):
    """Yield a single record, just for the newtabs"""

    vals = {'position': -1, 'tile_id': -1, 'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 1}

    del parts['tiles']
    cparts = parts.copy()
    cparts.update(vals)
    yield cparts


RULES = [
    InfernoRule(
        name='backfill_newtabs',
        source_tags=['processed:impression'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_date, parse_ip, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        # result_processor=partial(insert_postgres,
        #                          host='localhost',
        #                          database='mozsplice',
        #                          user='postgres',
        #                          password=PG_PASSWORD),
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=5432,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['date', 'position', 'locale', 'tile_id', 'country_code', 'os', 'browser',
                           'version', 'device', 'year', 'month', 'week'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked',
                             'sponsored', 'sponsored_link', 'newtabs'],
                table='impression_stats_daily',
            ),
        },
    ),
    InfernoRule(
        name='count_fetches',
        source_tags=['incoming:app'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        parts_preprocess=[count],
        geoip_file=GEOIP,
        combiner_function=combiner,
        keysets={
            'stats': Keyset(
                key_parts=['date', 'ver', 'locale', 'action'],
                value_parts=['count'],
            ),
        },
    ),
]
