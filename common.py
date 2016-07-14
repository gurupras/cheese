import hashlib

def sha256(string):
	m = hashlib.sha256()
	m.update(string)
	return m.hexdigest()

def sock_read(sock, n):
	buf = []
	while n > 0:
		data = sock.recv(n)
		if data == '':
			raise RuntimeError("Unexpected socket close")
		buf.append(data)
		n -= len(data)
	return ''.join(buf)
