#!/usr/bin/python

import sys
import re
import logging
from optparse import OptionParser
from math import sqrt


def read_hist(filename):
    total = 0.0
    histogram = [0]
    f = open(filename, "r")
    for line in f:
        chunk = re.split(',', line)
        count = int(chunk[1]) * 1.0
        total += count
        histogram.append(count)
    return total, histogram


def compute_freq_mean(freq, histogram):
    mean = 0
    for i in range(1, len(histogram)):
        mean += histogram[i] * (1 - (1 - freq) ** i)
    return mean


def estimate_freq(site_count, site, histogram):
    freq = 0.5
    left = 0.0000001
    right = 0.9999999
    while (right - left) > 0.0000001:
        mean = compute_freq_mean(freq, histogram)
        if mean < site_count:
            left = freq
        else:
            right = freq
        freq = (left + right) / 2
    return freq


def compute_distrib(site1, site2, sites, histogram):
    freq1 = sites[site1]
    freq2 = sites[site2]
    var = 0
    mean = 0
    for i in range(2, len(histogram)):
        mean += histogram[i] * (1 - (1 - freq1) ** i) * (1 - (1 - freq2) ** (i - 1))
        var += histogram[i] * (1 - (1 - freq1) ** i) * (1 - (1 - freq2) ** (i - 1)) * ((1 - freq2) ** (i - 1))
    # print site1, site2, mean , var
    # print "=========="
    return mean, var


def read_sites(filename, histogram):
    sites = {}
    f = open(filename, "r")
    for line in f:
        chunk = re.split(',', line)
        site = chunk[0]
        count = int(chunk[1]) * 1.0
        sites[site] = estimate_freq(count, site, histogram)
    return sites


def read_lines(sites, histogram):
    for line in sys.stdin:
        try:
            site1, site2, count = re.split(',', line)
            if site1 in sites and site2 in sites:
                mean, var = compute_distrib(site1, site2, sites, histogram)
                count = int(count)
                z = (count - mean) / sqrt(var)
                print "%s,%s,%d,%.2f" % (site1, site2, count, z)
                mean, var = compute_distrib(site2, site1, sites, histogram)
                z = (count - mean) / sqrt(var)
                print "%s,%s,%d,%.2f" % (site2, site1, count, z)
        except Exception as e:
            logging.error("Error processing line '%s': %s" % (line, e))


def read_args():
    parser = OptionParser()
    parser.add_option("-s", "--sites", dest="site_file", help="site counts")
    parser.add_option("-x", "--hist", dest="hist_file", help="history size historgram")
    return parser.parse_args()


if __name__ == '__main__':
    (options, args) = read_args()
    tot, hist = read_hist(options.hist_file)
    s = read_sites(options.site_file, hist)
    read_lines(s, hist)
    # read_lines(keys, value)

