import json
import threading
import socket
import select
import subprocess
import Queue
from os import path, devnull
from pigeon.logger import Logger
from pigeon.parser import JsonParser

QKCONNECT_EXE = "C:\\Users\\mribeiro\\qkthings_local\\build\\qt\\qkconnect\\release\\qkconnect.exe"

_DEVNULL = open(devnull, 'w')

class Conn(object):

    _NextID = 0

    StateInit = 0
    StateRunning = 1
    StateStopped = 2

    def __init__(self, type, params=[]):
        self.log = Logger()
        self._id = Conn._NextID
        self._type = type
        self._params = params
        self._state = Conn.StateInit
        self._callbacks = {
            "packet_received": None,
            "state_changed": None,
            "error": None
        }

    def set_callback(self, name, cb):
        self._callbacks[name] = cb

    def id(self):
        return self._id

    def state(self):
        return self._state

    def run(self):
        return True

    def stop(self):
        return True

    def send_packet(self, pkt):
        pass

    def _set_state(self, new_state):
        self._state = new_state
        if self._callbacks["state_changed"] is not None:
            self._callbacks["state_changed"](self._state)

    def _error(self, text):
        self.log.error(text)
        if self._callbacks["error"] is not None:
            self._callbacks["error"](text)

class QkConnect(Conn):

    _nextID = 0
    _next_port = 1238

    def __init__(self, port, type, params=[]):
        super(QkConnect, self).__init__(type, params)
        self._hostname = "localhost"
        self._port = port
        self._json_parser = JsonParser()
        self._json_parser.set_callback("parsed", self._handle_json)
        self._client = None
        self._packets_queue = Queue.Queue()
        self._packets_thread = None
        self._listener_thread = None
        self._alive = False
        self._event = threading.Event()

        self._process = None
        self._process_thread = None
        self._event.clear()

    def __del__(self):
        print "Deleting QkConnect %d" % self._id

    def __repr__(self):
        id_hex = hex(self._id)[2:0].zfill(4)
        return "%s %s %s" % (id_hex, self._type, self._params)




    def run(self):
        self._event.clear()
        qkconnect_exe = path.normpath(QKCONNECT_EXE)
        cmd = [qkconnect_exe, self._hostname, "%d" % self._port, self._type]
        if len(self._params):
            cmd += self._params
        cmd.append("--verbose")
        self.log.debug("%s CMD: %s" % (self.__class__.__name__, cmd))
        self._process = subprocess.Popen(
            cmd,
            stdin=_DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1
        )
        self._process_thread = threading.Thread(target=self._process_listener, args=[self._process.stdout])
        self._process_thread.setDaemon(True)
        self._process_thread.start()

        if self._event.wait(10.0) is False:
            self._error("Failed to create QkConnect")
            return False

        QkConnect._nextID += 1
        self._connect()
        self._set_state(Conn.StateRunning)
        return True

    def _connect(self):
        self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._client.connect((self._hostname, self._port))
        self._packets_thread = threading.Thread(target=self._handle_packets)
        self._packets_thread.setDaemon(True)
        self._listener_thread = threading.Thread(target=self._listener)
        self._listener_thread.setDaemon(True)
        self._alive = True
        self._packets_thread.start()
        self._listener_thread.start()

    def stop(self):
        self._alive = False
        self._client.close()
        self._packets_queue.put({"dum": "my"})
        self._packets_thread.join()
        self._listener_thread.join()
        self._process.terminate()
        self._set_state(Conn.StateStopped)

    def _handle_json(self, json_str):
        json_data = json.loads(json_str)
        json_keys = json_data.keys()

        if "pkt" in json_keys:
            json_data.update({
                "conn": {
                    "id": self._id
                }
            })
            if self._callbacks["packet_received"] is not None:
                self._callbacks["packet_received"](json_data)
        else:
            self._error("Unknown JSON format: %s" % json_str)

    def _process_listener(self, stdout):
        while self._process.poll() is None:
            for line in iter(stdout.readline, b""):
                nline = line.rstrip("\r\n")
                if "ready" in nline:
                    self.log.debug("QkConnect is READY")
                    self._event.set()

                self.log.debug("STDOUT %s %s: %s" % (self, self._port, nline))

    def _listener(self):
        while self._alive:
            input,output,err = select.select([self._client], [], [])
            for rd in input:
                if self._alive is True:
                    data = rd.recv(256)
                    self._json_parser.parse_data(data)

    def _handle_packets(self):
        while self._alive:
            pkt = self._packets_queue.get()
            if "pkt" in pkt:
                pkt_json_str = json.dumps(pkt)
                self._client.send(pkt_json_str)

    def send_packet(self, pkt):
        self.log.debug("SEND PACKET: %s" % pkt)
        self._packets_queue.put(pkt)

class ConnFactory(object):

    _next_qkconnect_port = 1238

    def __init__(self):
        self._conns = []

    def create(self, type, params={}):
        conn = QkConnect(ConnFactory._next_qkconnect_port, type, params)
        if conn.run():
            self._conns.append(conn)
            ConnFactory._next_qkconnect_port += 2
            return conn
        else:
            del conn
            return None

    def destroy(self, conn):
        conn.stop()
        self._conns.remove(conn)

    def conns(self):
        return self._conns

    def find(self, conn_id):
        for conn in self._conns:
            if conn.id() == conn_id:
                return conn
        return None
