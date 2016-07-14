import os,sys,argparse
import json
import time
import signal
import struct
import socket
import subprocess
import traceback

import threading
import multiprocessing

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

import common
import protocol_pb2

def setup_parser():
	parser = argparse.ArgumentParser()

	parser.add_argument('--host', '-H', type=str, required=True,
			help='Host to connect to')
	parser.add_argument('--port', '-p', type=int, default=41938,
			help='Port on which host is running server')
	parser.add_argument('--ncpus', '-j', type=int, default=multiprocessing.cpu_count(),
			help='Number of jobs to run in parallel')
	return parser

def handle_response(client_socket, queue, **kwargs):
	length = struct.unpack('>Q', common.sock_read(client_socket, 8))[0]
	logger.debug("Response length: %d" % (length))

	msg = common.sock_read(client_socket, length)
	logger.info("Received response")

	response = protocol_pb2.Response()
	response.ParseFromString(msg)

	if response.status != protocol_pb2.OK:
		logger.error(response.error)
		return
	else:
		logger.debug("OK")

	result = False
	if response.type == protocol_pb2.Response.GENERIC:
		result = True
	if response.type == protocol_pb2.Response.JOB_RESPONSE:
		# Read cmdline and start job to execute it
		cmdline = response.jobResponse.cmdline
		POOL.apply_async(func=run_cmdline, args=(cmdline, queue, True))
		#run_cmdline(cmdline, queue)
		result = True
	return result


def issue_job_request(socket):
	logger.debug("Issuing job request")
	# Make a request for a new job to run
	request = protocol_pb2.Request()
	request.type = protocol_pb2.Request.JOB_REQUEST
	request.jobRequest.type = protocol_pb2.JobRequest.JOB_REQUEST
	send_request(socket, request)


def run_cmdline(cmdline, queue, signal_fix):
	if signal_fix:
		signal.signal(signal.SIGINT, signal.SIG_IGN)
	ret, stdout, stderr = pycommons.run(cmdline)
	queue.put((cmdline, ret, stdout, stderr))

def response_handler(socket, queue):
	while True:
		handle_response(socket, queue)


def send_request(socket, request):
	msg = request.SerializeToString()
	length = struct.pack('>Q', len(msg))
	logger.debug("Request length: %d" % (len(msg)))
	socket.sendall(length + msg)
	logger.info("Request sent")

def client(host, port, ncpus, **kwargs):
	try:
		client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		client_socket.connect((host, port))
	except Exception:
		logger.critical('Could not connect to server!')
		logger.critical(traceback.format_exc())
		return


	request = protocol_pb2.Request()
	request.type = protocol_pb2.Request.REGISTER_CLIENT_REQUEST
	request.registerClientRequest.ncpus = ncpus
	send_request(client_socket, request)
	logger.info("Register client request sent")
	if handle_response(client_socket, None, **kwargs) is False:
		logger.error("Failed to register client")
		sys.exit(-1)

	manager = multiprocessing.Manager()
	queue = manager.Queue()

	t = threading.Thread(target=response_handler, args=(client_socket, queue))
	t.start()

	for j in range(ncpus):
		issue_job_request(client_socket)
	# Beyond this point, the handle_response function will take care
	# of running the actual jobs. So we just handle the responses

	while True:
		cmdline, ret, stdout, stderr = queue.get()
		request = protocol_pb2.Request()
		request.type = protocol_pb2.Request.JOB_COMPLETE_REQUEST
		request.jobCompleteRequest.cmdline = cmdline
		request.jobCompleteRequest.ret = ret
		request.jobCompleteRequest.stdout = stdout
		request.jobCompleteRequest.stderr = stderr
		send_request(client_socket, request)

		issue_job_request(client_socket)

def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	global POOL
	POOL = multiprocessing.Pool(args.ncpus)


	client(**args.__dict__)

if __name__ == '__main__':
	main(sys.argv)

