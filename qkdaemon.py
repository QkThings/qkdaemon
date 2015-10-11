import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.ioloop import IOLoop
import socket
import signal
import json
import re
import select
import threading
import subprocess
from os import path, devnull
import Queue
import time

QKCONNECT_EXE = "C:\\Users\\mribeiro\\qkthings_local\\build\\qt\\qkconnect\\release\\qkconnect.exe"

_DEVNULL = open(devnull, 'w')

#from tornado.tcpserver import TCPServer
import tornado.tcpserver


class Logger():
    NONE = 0
    INFO = 0x01
    WARNING = 0x02
    ERROR = 0x04
    DEBUG = 0x08
    ALL = 0x0FF

    default_levels = ALL

    _logs = []

    def __init__(self):
        self._levels = Logger.default_levels  # all by default
        self._verbose = False
        self._custom_logger = None
        Logger._logs.append(self)

    def __del__(self):
        Logger._logs.remove(self)

    @staticmethod
    def global_set_levels(levels):
        for log in Logger._logs:
            log.set_levels(levels)

    @staticmethod
    def global_set_logger(custom_logger):
        for log in Logger._logs:
            log.set_logger(custom_logger)

    @staticmethod
    def global_set_verbose(verbose):
        for log in Logger._logs:
            log.set_verbose(verbose)



    def set_levels(self, levels):
        self._levels = levels

    def set_logger(self, custom_logger):
        self._custom_logger = custom_logger

    def set_verbose(self, verbose):
        self._verbose = verbose

    def info(self, message):
        self._log(Logger.INFO, message)

    def warning(self, message):
        self._log(Logger.WARNING, message)

    def error(self, message):
        self._log(Logger.ERROR, message)

    def debug(self, message):
        self._log(Logger.DEBUG, message)

    def _log(self, type, message):
        if (type & self._levels) == 0:
            return

        timestamp = time.strftime("%Y/%m/%d %H:%M:%S")
        type_str = "[?????]"
        if type is Logger.INFO:
            type_str = "[INFO] "
        elif type is Logger.WARNING:
            type_str = "[WARN] "
        elif type is Logger.ERROR:
            type_str = "[ERROR]"
        elif type is Logger.DEBUG:
            type_str = "[DEBUG]"

        text = "%s %s %s" % (timestamp, type_str, message)

        if self._custom_logger is not None:
            if self._verbose:
                print(text)
            self._custom_logger(text)
        else:
            print(text)


class TCPServer(tornado.tcpserver.TCPServer):

    def _handle_data(self, data):
        print "RX:", data
        self._stream.read_bytes(32, self._handle_data, partial=True)

    def handle_stream(self, stream, address):
        print address, stream
        self._stream = stream
        stream.read_bytes(32, self._handle_data, partial=True)
        #self._stream = stream
        #self._read_line()

    def _read_line(self):
        self._stream.read_until('\n', self._handle_read)

    def _handle_read(self, data):
        self._stream.write(data)
        self._read_line()

class DataParser(object):
    def __init__(self):
        self.parsed_str = ""
        self._callback = {
            "parsed": None
        }

    def set_callback(self, name, cb):
        self._callback[name] = cb

    def clear(self):
        self.parsed_str = ""


class SerialParser(DataParser):
    def __init__(self):
        super(SerialParser, self).__init__()

    def parse_data(self, data):
        for ch in data:
            if ch == '\n':
                if self._callback["parsed"] is not None:
                    self._callback["parsed"](self.parsed_str)
                    self.parsed_str = ""
            else:
                self.parsed_str += ch


class JsonParser(DataParser):
    def __init__(self):
        super(JsonParser, self).__init__()
        self._parse_level = 0
        self._in_str = False
        self._esc_char = False

    def parse_data(self, data):
        done = False
        for ch in data:
            if ch == '\"':
                if not self._in_str:
                    self._in_str = True
                else:
                    if not self._esc_char:
                        self._in_str = False
                    else:
                        self._esc_char = False

            if self._in_str:
                if ch == "\\":
                    self._esc_char = True
            else:
                if ch == '{':
                    if self._parse_level == 0:
                        self.parsed_str = ""
                    self._parse_level += 1
                elif ch == '}':
                    self._parse_level -= 1
                    if self._parse_level == 0:
                        done = True

            self.parsed_str += ch

            if done and self._callback["parsed"] is not None:
                self._callback["parsed"](self.parsed_str)

class RPCServer(object):

    def __init__(self):
        self._rpc_thread = threading.Thread(target=self._run)
        self._rpc_thread.daemon = True
        self._tcp_listener = threading.Thread(target=self._listener)
        self._tcp_listener.setDaemon(True)
        self._server = None
        self._alive = False
        self._request_to_quit = False
        self._clients = []
        self._lock = threading.Lock()
        self._queue = Queue.Queue()
        self.log = Logger()
        self.rpc_prefix = "rpc_"

    def _run(self):
        self.log.info("%s is running..." % self.__class__.__name__)
        while self._request_to_quit is False:
            rpc = self._queue.get()
            if "rpc" in rpc.keys():
                self._handle_rpc(rpc)

    def run(self, cli=False):
        if cli:
            self._rpc_thread.start()
            while True:
                input_text = raw_input()
                self.handle_rpc_serial_data(input_text)
                if input_text == "quit":
                    break
            self._rpc_thread.join()
        else:
            self._run()

    def rpc_quit(self):
        self._request_to_quit = True

    def _handle_message(self, text):
        self.log.debug("Message: %s" % text)
        for skt in self._clients:
            skt.send(json.dumps(text))

    def message(self, text, params={}, meta=""):
        self._handle_message({"message": {
            "origin": self.__class__.__name__,
            "meta": meta,
            "text": text,
            "params": params,
            }})

    def connect(self, hostname, port):
        self._alive = True
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.bind((hostname, port))
        self._server.listen(1)
        self._tcp_listener.start()
        self.message("", meta="server.connected")

    def disconnect(self):
        self._alive = False
        #self._thread_listener.join(10000)
        self.message("", meta="server.disconnected")
        self._server.fileno()
        #self._server.shutdown(socket.SHUT_RD)
        self._server.close()

    def _listener(self):
        while self._alive:
            rd, wr, err = select.select([self._server], [], [])
            for s in rd:
                if s is self._server:
                    client_skt, client_addr = self._server.accept()
                    self._lock.acquire()
                    self._clients.append(client_skt)
                    self._lock.release()
                    threading._start_new_thread(self._handle_client, (client_skt, client_addr))
        self._server.close()
        print("Server closed")

    def use_prefix(self, prefix):
        self.rpc_prefix = prefix

    def _handle_rpc(self, rpc):
        if "rpc" not in rpc.keys():
            # Invalid RPC. And that's fine.
            self.log.debug("invalid RPC: \"rpc\" element not found")
            return
        rpc = rpc["rpc"]
        rpc_method = rpc["method"]
        rpc_params = rpc["params"]

        try:
            rpc_cb = getattr(self, self.rpc_prefix + rpc_method)
        except Exception, e:
            self.log.error("RPC not implemented: %s" % rpc_method)
            return

        rpc_cb(**rpc_params)


    def handle_rpc_serial_data(self, data):
            data = data.replace('\r', '')
            data = data.replace('\n', '')
            fields = data.split(' ')
            if len(fields) < 1:
                self.log.debug("Invalid DATA:", data)
            else:
                method = fields[0]
                params = {}
                for param in fields[1:]:
                    param_fields = param.split("=")
                    if len(param_fields) == 2:
                        param_name = param_fields[0]
                        param_value = param_fields[1]
                        if param_value.isdigit():
                            param_value = int(param_value)
                        params.update({param_name: param_value})
                rpc_pkt = {
                    "rpc": {
                        "method": method,
                        "params": params,
                    }
                }
                self._queue.put(rpc_pkt)

    def _handle_client(self, skt, addr):
        self.log.info("Client connected: %s %d" % (addr[0], skt.fileno()))

        data_parser = SerialParser()
        data_parser.set_callback("parsed", self.handle_rpc_serial_data)
        while 1:
            try:
                data = skt.recv(1024)
                if not data:
                    break
                data_parser.parse_data(data)
            except Exception, e:
                self.log.error(str(e))
                break
        self._lock.acquire()
        self._clients.remove(skt)
        self._lock.release()
        self.log.info("Client disconnected: %s %d" % (addr[0], skt.fileno()))
        skt.close()




class APIResource(object):

    SET = 0x01
    GET = 0x02

    def __init__(self, name, attr, callback, parent=None, children=[]):
        self.name = name
        self.cb = callback
        self.attr = attr
        self.parent = parent
        self.children = children

    def set_parent(self, parent):
        self.parent = parent

    def add_child(self, resource):
        self.children.append(resource)


class APIHandler(object):


    def __init__(self, filepath):
        self.callbacks = {}
        self.log = Logger()
        self._root = None
        self.uri = ""
        self.method = ""
        self.data = {}
        self.parsed_params = {}
        self._create(filepath)

    def _create(self, filepath):
        file = open(filepath, "r")

        max_level = 0
        for line in file:
            level = line.count('-')
            if level > max_level:
                max_level = level
        file.close()
        parents = [None]*max_level

        file = open(filepath, "r")
        for line in file:
            line = line[:-1]
            if line[0] == '#':
                continue

            line = line.replace(' ', '')
            line = line.replace('\t', '')
            fields = line.split(',')

            if len(fields) != 3:
                print "ERROR parsing %s (%s)" % (filepath, line)
                continue

            level = fields[0].count('-')

            rsrc_name = fields[0].replace('-','')
            rsrc_attr = 0
            if 's' in fields[1]: rsrc_attr |= APIResource.SET
            if 'g' in fields[1]: rsrc_attr |= APIResource.GET
            cb_name = fields[2]

            cb = None
            if "*" in cb_name:
                print "Callback of %s is intentionally not implemented" % rsrc_name
            else:
                try:
                    cb = getattr(self, "api_" + cb_name)
                except:
                    raise Exception("Callback %s not implemented" % cb_name)

            api_rsrc = APIResource(rsrc_name, rsrc_attr, cb)

            if level == 0:
                if self._root is None:
                    self._root = api_rsrc
                else:
                    raise Exception("Multiple roots")
            else:
                parent = parents[level-1]
                if parent is None:
                    raise Exception("%s has no parent" % rsrc_name)
                api_rsrc.set_parent(parent)
                parent.add_child(api_rsrc)

            parents[level] = api_rsrc

    def run(self, uri, method, data={}):
        self.uri = uri

        if method != "get" and method != "set":
            raise Exception("Method must be \"get\" or \"set\"")
        self.method = method

        resources = uri.split('/')
        resources = filter(None, resources)
        print "URI:", resources

        self.parsed_params.clear()
        cur_rsrc = self._root
        first_rsrc_name = resources[0]

        if first_rsrc_name != cur_rsrc.name:
            print "%s is not the name of the first resource (%s)" % (first_rsrc_name,cur_rsrc.name)
            return

        for rsrc_name in resources[1:]:

            next_rsrc = None
            for child in cur_rsrc.children:
                if child.name == rsrc_name:
                    next_rsrc = child
                    break

            if next_rsrc is None:
                param_re = re.compile("\\{.*?\\}")
                for child in cur_rsrc.children:
                    if bool(param_re.search(child.name)):
                        print "PARSED VALUE %s %s" % (child.name, rsrc_name)
                        param_name = child.name
                        param_name = param_name.replace('{', '')
                        param_name = param_name.replace('}', '')
                        param_value = int(rsrc_name)
                        self.parsed_params.update({param_name: param_value})
                        next_rsrc = child
                        break

            if next_rsrc is not None:
                cur_rsrc = next_rsrc
            else:
                print "Parsing error: resource %s not found" % rsrc_name
                return

        if (method == "get" and (cur_rsrc.attr & APIResource.GET) == 0) or \
           (method == "set" and (cur_rsrc.attr & APIResource.SET) == 0):
            print "Unsupported method name: %s %s" % (method, rsrc_name)
            return

        if cur_rsrc.cb is not None:
            print "Call %s" % cur_rsrc.cb.__name__
            if self._validate_parsed_params() is True:
                cur_rsrc.cb()
        else:
            print "Callback of %s is not implemented" % cur_rsrc.name
            return

    def _validate_parsed_params(self):
        return True


    def set(self, uri, data={}):
        self.run(uri, "set", data)

    def get(self, uri, data={}):
        self.run(uri, "get", data)

class QkConnect(object):

    _nextID = 0

    def __init__(self, port, type, params={}):
        self.log = Logger()
        self._id = QkConnect._nextID
        self._hostname = "localhost"
        self._port = port
        self._type = type
        self._params = params
        self._client = None
        self._thread_listener = None
        self._alive = False
        self._json_parser = JsonParser()
        self._json_parser.set_callback("parsed", self._handle_json)
        self._event = threading.Event()

        self._process = None
        self._process_thread = None
        self._event.clear()
        self._callbacks = {
            "packet_received": None
        }

    def __del__(self):
        print "Deleting QkConnect %d" % self._id

    def __repr__(self):
        id_hex = hex(self._id)[2:0].zfill(4)
        return "%s %s %s" % (id_hex, self._type, self._params)

    def id(self):
        return self._id

    def set_callback(self, name, cb):
        self._callbacks[name] = cb

    def run(self):
        self._event.clear()
        qkconnect_exe = path.normpath(QKCONNECT_EXE)
        cmd = [qkconnect_exe, self._hostname, "%d" % self._port, self._type, "--verbose"]
        self.log.debug("%s CMD: %s" % (self.__class__.__name__, cmd))
        self._process = subprocess.Popen(
            cmd,
            stdin=_DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1
        )
        self._process_thread = threading.Thread(target=self._process_listener,args=[self._process.stdout])
        self._process_thread.setDaemon(True)
        self._process_thread.start()

        if self._event.wait(3.0) is False:
            self.log.error("Failed to create QkConnect")
            return False

        QkConnect._nextID += 1
        self._connect()
        return True

    def _connect(self):
        self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._client.connect((self._hostname, self._port))
        self._thread_listener = threading.Thread(target=self._listener)
        self._thread_listener.setDaemon(True)
        self._alive = True
        self._thread_listener.start()


    def terminate(self):
        self._alive = False
        self._client.close()
        self._thread_listener.join()
        self._process.terminate()

    def _handle_json(self, json_str):
        json_data = json.loads(json_str)
        json_keys = json_data.keys()
        #TODO add timestamps to packets
        if "pkt" in json_keys:
            json_data.update({
                "conn": {
                    "id": self._id
                }
            })
            if self._callbacks["packet_received"] is not None:
                self._callbacks["packet_received"](json_data)
        else:
            self.log.error("Unknown JSON format: %s" % json_str)

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

    def send_packet(self, pkt):
        pkt_json_str = json.dumps(pkt)
        self._client.send(pkt_json_str)

class QkConnectFactory(object):

    _next_port = 1238

    def __init__(self):
        self._conns = []

    def create(self, type, params={}):
        conn = QkConnect(QkConnectFactory._next_port, type, params)
        if conn.run():
            self._conns.append(conn)
            QkConnectFactory._next_port += 2
            return conn
        else:
            del conn
            return None

    def destroy(self, conn_id):
        conn = self.find(conn_id)
        if conn:
            conn.terminate()
            self._conns.remove(conn)

    def conns(self):
        return self._conns

    def find(self, conn_id):
        for conn in self._conns:
            if conn.id() == conn_id:
                return conn
        return None




class QkDaemonAPI(APIHandler):

    def __init__(self, filepath, qk):
        super(QkDaemonAPI, self).__init__(filepath)
        self._qk = qk

    def _build_packet(self, name, params={}):
        pkt = {}
        if name == "search":
            pkt.update({
                "code": "search"
            })
        else:
            raise Exception("unknown packet name (%s)" % name)

        if params:
            pkt.update({"params": params})

        pkt = {"pkt": pkt}
        return pkt

    def _send_packet(self, conn_id, name, params={}):
        conn = self._qk.find_conn(conn_id)
        if conn is not None:
            pkt = self._build_packet(name, params)
            conn.send_packet(pkt)
        else:
            print "ERROR: conn not found"

    def _validate_parsed_params(self):
        return True
        params = self.parsed_params
        param_keys = params.keys()
        if "conn_id" in param_keys:
            conn_id = params["conn_id"]
            if self._qk.find_conn(conn_id):
                return True
            else:
                print "ERROR: invalid conn_id %d" % conn_id
                return False
        return True

    def api_qk(self):
        pass

    def api_conns(self):
        pass

    def api_conn(self):
        pass

    def api_subscriptions(self):
        pass

    def api_subscribe_data(self):
        pass

    def api_subscribe_event(self):
        pass

    def api_subscribe_debug(self):
        pass

    def api_cmds(self):
        pass

    def api_cmd_search(self):
        print "SEARCH on conn_id=%d" % self.parsed_params["conn_id"]
        conn_id = self.parsed_params["conn_id"]
        self._send_packet(conn_id, "search")

    def api_nodes(self):
        pass

    def api_node(self):
        pass

    def api_comm(self):
        pass

    def api_device(self):
        pass

    def api_board_info(self):
        pass

    def api_board_configs(self):
        pass

    def api_board_config(self):
        pass

    def api_board_cmds(self):
        pass

    def api_board_cmd_update(self):
        pass

    def api_board_cmd_save(self):
        pass

    def api_device_data(self):
        pass

    def api_device_events(self):
        pass

    def api_device_actions(self):
        pass

    def api_device_action(self):
        pass


class QkDaemon(RPCServer):

    _next_qkconnect_port = 1248

    def __init__(self, apipath):
        super(QkDaemon, self).__init__()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self._qkconnect_factory = QkConnectFactory()
        self._api = QkDaemonAPI(apipath, self)
        self._tcp_server_thread = None
        self._tcp_server_port = 1234
        self._tcp_server_ioloop = None

    def api(self):
        return self._api

    def _run_tcp_server(self, port):
        self.log.info("Running TCP server on port %d" % port)
        server = TCPServer()
        server.listen(port)
        self._tcp_server_ioloop = IOLoop.current()
        self._tcp_server_ioloop.start()

    def _handle_signal(self, sig, frame):
        #TODO stop all servers
        #IOLoop.current().add_callback(IOLoop.current().stop)
        pass

    def _handle_packet_received_from_conn(self, pkt):
        self.log.debug("Send packet to clients: %s" % pkt)

    def find_conn(self, conn_id):
        return self._qkconnect_factory.find(conn_id)

    def start_tcp_server(self, port):
        if self._tcp_server_thread is None:
            self._tcp_server_thread = threading.Thread(target=self._run_tcp_server,
                                                       args=[port])
            self._tcp_server_thread.setDaemon(True)
            self._tcp_server_thread.start()
        else:
            self.log.error("TCP server is already running")

    def stop_tcp_server(self):
        if self._tcp_server_thread is not None:
            IOLoop.instance().stop()
            self._tcp_server_thread.join()


    def rpc_test(self):
        print "THIS IS A TEST"
        self._api.set("/qk/conns/0/search")

    def rpc_api(self, uri, method, params={}):
        self._api.run(uri, method, params)

    def rpc_add_conn(self, type, params={}):
        conn = self._qkconnect_factory.create(type, params)
        if conn:
            conn.set_callback("packet_received", self._handle_packet_received_from_conn)
            self.log.info("Connection added: %s" % conn)
        else:
            self.log.error("Failed to add connection")
        return conn


    def rpc_remove_conn(self, id):
        conn = self._find_conn(id)
        if conn:
            self._conns.remove(conn)
        else:
            self.log.error("Failed to find connection")

    def rpc_list_conn(self):
        conns = self._qkconnect_factory.conns()
        if conns:
            for conn in conns:
                self.log.info("%s" % conn)
        else:
            self.log.info("Connections list is empty")

    def rpc_quit(self):
        super(QkDaemon, self).rpc_quit()
        self.stop_tcp_server()


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print("WebSocket opened")

    def on_message(self, message):
        try:
            json_req = json.loads(message)
            print "rx:" % message, json_req
            #self.write_message(u"You said: " + message)
        except ValueError, e:
            print "ERROR: %s (\"%s\")" % (e.message, message)

    def on_close(self):
        print("WebSocket closed")


class WebRequestHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

application = tornado.web.Application([
    (r"/", WebRequestHandler),
    (r"/ws", WebSocketHandler)
])


def main():
    print "============================================================"
    print " QkDaemon | qkthings.io"
    print "============================================================"

    qk = QkDaemon("resources/api/api.qkapi")
    #qk.start_tcp_server(1234)
    qk.connect("localhost", 1111)
    qk.run(True)
    qk.disconnect()

    #application.listen(8888)
    #IOLoop.current().start()

if __name__ == "__main__":
    main()

