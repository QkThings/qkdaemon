import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.ioloop import IOLoop
import signal
import json
import threading
import Queue

import tornado.tcpserver
import pigeon.server
import pigeon.parser
import pigeon.logger
from api import APIHandler
from conn import ConnFactory


class QkDaemonAPISerialParser(pigeon.parser.SerialParser):

    def __init__(self):
        super(QkDaemonAPISerialParser, self).__init__()
        self._callback.update({
            "api_parsed": None
        })

    def set_callback(self, name, cb):
        self._callback[name] = cb

    def handle_parsed_data(self, data):
        data = str(data)
        data = data.replace('\r', '')
        data = data.replace('\n', '')
        fields = data.split(' ')
        if len(fields) < 2:
            self.log.error("Invalid API data")
        method = fields[0]
        uri = fields[1]
        params = {}
        if len(fields) > 2:
            params_str = fields[2]
            try:
                params = json.loads(params_str)
            except:
                params = {}
                self.log.error("Failed to decode request's parameters")

        if self._callback["api_parsed"] is not None:
            self._callback["api_parsed"](method, uri, params)



class QkDaemonAPIJsonParser(pigeon.parser.JsonParser):
    pass



class QkDaemonTCPServer(pigeon.server.TCPServer):

    def __init__(self, qk):
        super(QkDaemonTCPServer, self).__init__()
        self._qk = qk
        self._qk_clients = []

    def clients(self):
        return self._qk_clients

    def handle_client(self, skt, addr):

        client = QkDaemonTCPClient(self._qk, skt, addr)
        self._lock.acquire()
        self._qk_clients.append(client)
        self._lock.release()

        client.run()

        self._lock.acquire()
        self._qk_clients.remove(client)
        self._lock.release()


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


class QkDaemonAPI(APIHandler):

    _next_packet_id = 1

    def __init__(self, filepath, qk):
        super(QkDaemonAPI, self).__init__(filepath)
        self._qk = qk
        self._waiting_acks = []

    @staticmethod
    def _request_id():
        id = QkDaemonAPI._next_packet_id
        if QkDaemonAPI._next_packet_id < 254:
            QkDaemonAPI._next_packet_id += 1
        else:
            QkDaemonAPI._next_packet_id = 1
        return id


    def _build_packet(self, name, params={}):
        pkt = {
            "code": name,
            name: params
        }

        pkt.update({"id": QkDaemonAPI._request_id()})
        return pkt

    def _send_packet(self, conn_id, name, params={}):
        conn = self._qk.find_conn(conn_id)
        if conn is not None:
            pkt = self._build_packet(name, params)
            self._waiting_acks.append(pkt["id"])
            pkt = {"pkt": pkt}
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

    def waiting_acks(self):
        return self._waiting_acks

    def ack(self, id):
        self._waiting_acks.remove(id)

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


class QkDaemonTCPClient(object):

    def __init__(self, qk, skt, addr):
        super(QkDaemonTCPClient, self).__init__()
        self.log = pigeon.logger.Logger()
        self._qk = qk
        self._skt = skt
        self._addr = addr
        self._api = qk.api()#QkDaemonAPI("resources/api/api.qkapi", qk)
        self._parser = QkDaemonAPISerialParser()
        self._parser.set_callback("api_parsed", self._api.run)

    def __repr__(self):
        return "%s skt:%d" % (self.__class__.__name__, self._skt.fileno())

    def api(self):
        return self._api

    def send_data(self, data):
        self._skt.send(data)

    def run(self):
        while 1:
            try:
                data = self._skt.recv(1024)
                if not data:
                    break
                self._parser.parse_data(data)
            except Exception, e:
                self.log.error(str(e))
                break



class QkDaemon(pigeon.server.PigeonServer):

    def __init__(self, apipath):
        super(QkDaemon, self).__init__()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self._conn_factory = ConnFactory()
        self._api = QkDaemonAPI(apipath, self)
        self._tcp_server_thread = None
        self._tcp_server = QkDaemonTCPServer(self)
        self._packets_received_queue = Queue.Queue()
        self._packets_received_thread = threading.Thread(target=self._handle_packets_received_from_conns)
        self._packets_received_thread.setDaemon(True)
        self.use_prefix("qk_")

    def api(self):
        return self._api

    def _handle_signal(self, sig, frame):
        #TODO stop all servers
        #IOLoop.current().add_callback(IOLoop.current().stop)
        pass

    def run(self, cli=False):
        self._packets_received_thread.start()
        super(QkDaemon, self).run(cli)
        self._packets_received_thread.join()


    def _handle_api(self, method, uri, params={}):
        req = {
            "method": method,
            "uri": uri,
            "params": params
        }
        print("REQUEST: %s" % req)
        self._packets_received_queue.put(req)

    def _handle_packets_received_from_conns(self):
        while self._request_to_quit is False:
            data = self._packets_received_queue.get()
            self.log.debug("Packet from conn: %s" % json.dumps(data))
            if "pkt" in data:
                pkt = data["pkt"]
                pkt_id = pkt["id"]
                pkt_keys = pkt.keys()
                if "ack" in pkt_keys:
                    ack = pkt["ack"]
                    #ack_status = ack["status"]
                for client in self._tcp_server.clients():
                    client_api = client.api()
                    waiting_acks = client_api.waiting_acks()
                    print("%s wait acks %s" % (client, waiting_acks))
                    #TODO hw MUST send ACK according to packet ID
                    #if pkt_id in waiting_acks:
                    if len(waiting_acks) > 0:
                        client_api.ack(pkt_id)

    def _handle_packet_received_from_conn(self, data):
        self._packets_received_queue.put(data)
        pkt = data["pkt"]

    def find_conn(self, conn_id):
        return self._conn_factory.find(conn_id)

    def start_tcp_server(self, port):
        if self._tcp_server.is_alive():
            self.log.error("TCP server is already running")
        else:
            self._tcp_server.bind("localhost", port)

    def stop_tcp_server(self):
        if self._tcp_server.is_alive():
            self._tcp_server.close()

    def qk_test(self):
        print "THIS IS A TEST"
        self._handle_api("set", "/qk/conns/0/cmds/search")

    def qk_api(self, method, uri, params):
        self._api.run(method, uri, json.loads(params))

    def qk_add_conn(self, type, params={}):
        conn = self._conn_factory.create(type, params)
        if conn:
            conn.set_callback("packet_received", self._handle_packet_received_from_conn)
            self.log.info("Connection added: %s" % conn)
        else:
            self.log.error("Failed to add connection")
        return conn


    def qk_remove_conn(self, conn_id):
        conn = self.find_conn(conn_id)
        if conn:
            self._conn_factory.destroy(conn)
        else:
            self.log.error("Failed to find connection with id=%d" % conn_id)

    def qk_list_conn(self):
        conns = self._conn_factory.conns()
        if conns:
            for conn in conns:
                self.log.info("%s" % conn)
        else:
            self.log.info("Connections list is empty")

    def qk_quit(self):
        super(QkDaemon, self).pgn_quit()
        self._packets_received_queue.put({"dum": "my"})
        self._packets_received_thread.join()
        self.stop_tcp_server()





def main():
    print "============================================================"
    print " QkDaemon | qkthings.io"
    print "============================================================"

    qk = QkDaemon("resources/api/api.qkapi")
    qk.start_tcp_server(1234)
    qk.bind("localhost", 1111)
    qk.run(True)

    #application.listen(8888)
    #IOLoop.current().start()

if __name__ == "__main__":
    main()

