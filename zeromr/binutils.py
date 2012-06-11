# -*- coding: utf-8 -*-
import argparse
import logging
from zeromr.colorizing import ColorizingStreamHandler

def get_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log', default='INFO')
    return parser

def setup_logger(args):
    root = logging.getLogger()
    root.setLevel(args.log.upper())
    stream_handler = ColorizingStreamHandler()
    stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(stream_handler)
