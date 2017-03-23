import os,sys,argparse
import signal
import time

import beanstalkc

import threading
import multiprocessing
from threading import Semaphore

import logging
try:
	import pycommons
	from pycommons import generic_logging
	if __name__ == '__main__':
		generic_logging.init(level=logging.DEBUG)
except:
	print 'No pycommons..continuing anyway'
	logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__file__)

sem = None

def setup_parser():
	parser = argparse.ArgumentParser()

	parser.add_argument('--host', '-H', type=str, required=True,
			help='Host to connect to')
	parser.add_argument('--port', '-p', type=int, default=15225,
			help='Port on which host is running server')
	parser.add_argument('--ncpus', '-j', type=int, default=multiprocessing.cpu_count(),
			help='Number of jobs to run in parallel')
	return parser

def run_cmdline(cmdline, queue, signal_fix):
	try:
		if signal_fix:
			signal.signal(signal.SIGINT, signal.SIG_IGN)
		ret, stdout, stderr = pycommons.run(cmdline)
		queue.put((cmdline, ret, stdout, stderr))
	except Exception, e:
		queue.put((cmdline, None, None, e))


def callback(self):
	sem.release()


def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	beanstalk = beanstalkc.Connection(host=args.host, port=args.port)

	pool = multiprocessing.Pool(args.ncpus)
	manager = multiprocessing.Manager()
	queue = manager.Queue()
	global sem
	sem = Semaphore(args.ncpus)

	def run_jobs():
		while True:
			j = beanstalk.reserve()
			print j.body
			sem.acquire()
			pool.apply_async(func=run_cmdline, args=(j.body, queue, True), callback=callback)
			#run_cmdline(j.body, queue, True)

	def process_queue():
		while True:
			cmdline, ret, stdout, stderr = queue.get()
			if stderr != None and stderr != '':
				print '{} failed: {}'.format(cmdline, stderr)
	
	jobs_t = threading.Thread(target=run_jobs)
	queue_t = threading.Thread(target=process_queue)

	jobs_t.start()
	queue_t.start()

	queue_t.join()
if __name__ == '__main__':
	main(sys.argv)
