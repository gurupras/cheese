import os,sys,argparse
import struct
import socket
import Queue

import threading
import signal

import logging
import pycommons
from pycommons import generic_logging
generic_logging.init(level=logging.INFO)
logger = logging.getLogger(__file__)

import common
import protocol_pb2

class Client(object):
	def __init__(self, socket, addr):
		self.socket = socket
		self.ip = addr[0]
		self.port = addr[1]
		self.jobs = []


class Container(object):
	LOCK = threading.Lock()
	FINISHED = 0
	NJOBS = 0
	JOBS = Queue.Queue()
	CLIENTS = {}
	IDLE_CLIENTS = []

def setup_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument('jobsfile', help='File containing jobs to run')
	parser.add_argument('--port', '-p', default=15225, type=int, help='Port to use')
	return parser


def send_response(sock, response):
	msg = response.SerializeToString()
	length = struct.pack(">Q", len(msg))
	logger.debug("Response length: %d" % (len(msg)))
	sock.sendall(length + msg)

def handle_register_client_request(sock, client, request, response):
	logger.info("handle_register_client_request")
	response.type = protocol_pb2.Response.GENERIC
	response.status = protocol_pb2.OK

def	handle_job_request(sock, client, request, response):
		logger.info("handle_job_request")
		response.status = protocol_pb2.OK
		if Container.JOBS.empty():
			logger.info("NO JOBS")
			response.type = protocol_pb2.Response.GENERIC
			Container.IDLE_CLIENTS.append(client)
		else:
			response.type = protocol_pb2.Response.JOB_RESPONSE
			job = Container.JOBS.get()
			logger.info("%s -> %s" % (job, client.ip))
			response.jobResponse.cmdline = job
			client.jobs.append(job)

def handle_job_complete_request(sock, client, request, response):
	response.type = protocol_pb2.Response.GENERIC
	if request.ret == 0:
		response.status = protocol_pb2.OK
	else:
		response.status = protocol_pb2.OK
		logger.error("FAILED!: %s" % (request.cmdline))
		d = {
			'cmdline' : request.cmdline,
			'ret' : request.ret,
			'stdout' : request.stdout,
			'stderr' : request.stderr
		}
		logger.error(json.dumps(d, indent=2))

	Container.LOCK.acquire()
	Container.FINISHED += 1
	logger.info("Finished: %d/%d (%d%%)" % (Container.FINISHED, Container.NJOBS, (Container.FINISHED * 100) / Container.NJOBS))
	Container.LOCK.release()
	client.jobs.remove(request.cmdline)

def handle(client):
	sock = client.socket
	ip, port = client.ip, client.port
	addr = (ip, port)
	try:
		while True:
			length = struct.unpack('>Q', common.sock_read(sock, 8))[0]
			logger.debug("Request length: %d" % (length))
			msg_buf = common.sock_read(sock, length)
			request = protocol_pb2.Request()
			request.ParseFromString(msg_buf)

			response = protocol_pb2.Response()
			if request.type == protocol_pb2.Request.REGISTER_CLIENT_REQUEST:
				handle_register_client_request(sock, addr, request.registerClientRequest, response)
			elif request.type == protocol_pb2.Request.JOB_REQUEST:
				handle_job_request(sock, client, request.jobRequest, response)
			elif request.type == protocol_pb2.Request.JOB_COMPLETE_REQUEST:
				handle_job_complete_request(sock, client, request.jobCompleteRequest, response)
			send_response(sock, response)
	except Exception, e:
		logger.error(str(e))
		logger.warning("A client(%s) failed! Releasing allocated jobs" % (client.ip))
		njobs = 0
		for j in client.jobs:
			Container.JOBS.put(j)
			njobs += 1
		assert Container.FINISHED + Container.JOBS.qsize() == Container.NJOBS, "Something's wrong..."
		# Create a fake request for idle clients so they don't sit idly
		request = protocol_pb2.Request()
		request.type = protocol_pb2.Request.JOB_REQUEST
		response = protocol_pb2.Response()
		for idle_client in Container.IDLE_CLIENTS:
			handle_job_request(idle_client.socket, idle_client, request, response)
			send_response(idle_client.socket, response)
			Container.IDLE_CLIENTS.remove(idle_client)


def server(port):
	server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	try:
		server_socket.bind(('', port))
	except socket.error as e:
		logger.error('Bind failed! :' + e[1])
		sys.exit(-1)

	server_socket.listen(10)

	while 1:
		sock, addr = server_socket.accept()

		client = Client(sock, addr)

		t = threading.Thread(target=handle, args=(client,))
		t.start()
#		while True:
#			handle(sock, addr)

def main(argv):
	parser = setup_parser()
	args = parser.parse_args(argv[1:])

	if not os.path.exists(args.jobsfile):
		logger.error("Jobsfile '%s' does not exist" % (args.jobsfile))
		sys.exit(-1)
	else:
		Container.NJOBS = 0
		with pycommons.open_file(args.jobsfile, 'rb') as f:
			for line in f:
				line = line.strip()
				if line != '':
					Container.JOBS.put(line)
					Container.NJOBS += 1

	server(args.port)

if __name__ == '__main__':
	main(sys.argv)
