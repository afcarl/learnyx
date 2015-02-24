#!/usr/bin/env python
import json
import sys
from random import shuffle

if __name__ == '__main__':
    for line in sys.stdin:
        d = json.loads(line)
        if 'ip' in d:
            ips = d['ip'].split('.')
            shuffle(ips)
            ip = '.'.join(ips)
            d['ip'] = ip
        print json.dumps(d)