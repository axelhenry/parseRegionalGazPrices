#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import yaml

import OpenDataRegionalGazPricesParser

parser = argparse.ArgumentParser(
    description='''parse an Xml file containing local gaz prices,
                compute the average for an area, and write results in database''')
parser.add_argument(
    '--conf_file', '-cf',
    help='path to config file, if not provided config.yaml will be used')
# parser.add_argument(
#     '--url', '-u', help='url to get our file', required=True)
# parser.add_argument('--directory', '-d',
#                     help='path to directory containing our results\' files',
#                     required=True)
# parser.add_argument('--departments', '-dpts',
#                     help='path to file containing the key to be used',
#                     required=True)
args = parser.parse_args()
if args.conf_file:
    with open(args.conf_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
else:
    with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

myParser = OpenDataRegionalGazPricesParser.OpenDataRegionalGazPricesParser(
    cfg['parser']['url'], cfg['parser']['directory'], cfg['postgresql'])
