import os,sys,argparse

import logging
import pycommons
from pycommons import generic_logging
generic_logging.init(level=logging.INFO)
logger = logging.getLogger(__file__)

import beanstalkc

def setup_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument('jobsfile', help='File containing jobs to run')
	parser.add_argument('--host', '-H', type=str, default='localhost',
			help='Host to connect to')
	parser.add_argument('--port', '-p', default=15225, type=int, help='Port to use')
	return parser

def process(jobsfile, host, port, *args, **kwargs):
	beanstalk = beanstalkc.Connection(host=host, port=port)

	with pycommons.open_file(jobsfile, 'rb') as f:
		for line in f:
			data = line.strip()
			beanstalk.put(data)



def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

if __name__ == '__main__':
	main(sys.argv)
