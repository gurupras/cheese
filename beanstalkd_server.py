import os,sys,argparse
import time

import threading
import subprocess
import shlex

import logging
import pycommons
from pycommons import generic_logging
generic_logging.init(level=logging.INFO)
logger = logging.getLogger(__file__)

import beanstalkd_push_work

def setup_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument('jobsfile', help='File containing jobs to run')
	parser.add_argument('--port', '-p', default=15225, type=int, help='Port to use')
	return parser

def process_subprocess_stdout(p):
	for line in iter(p.stdout.readline, b''):
		print line.strip()

def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	cmdline = shlex.split('/usr/local/bin/beanstalkd -p {} -V'.format(args.port))
	p = subprocess.Popen(cmdline, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	t = threading.Thread(target=process_subprocess_stdout, args=(p,))
	t.start()

	time.sleep(1)
	beanstalkd_push_work.process(args.jobsfile, 'localhost', args.port)

	t.join()
	p.wait()

if __name__ == '__main__':
	main(sys.argv)
