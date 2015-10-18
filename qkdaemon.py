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



class TCPServer(pigeon.server.TCPServer):

    ModeSerial = 0
    ModeJSON = 1

    def __init__(self, mode):
        super(TCPServer, self).__init__()
        self._mode = mode
        self._callbacks = {
            "handle_api": None
        }

    def set_callback(self, name, cb):
        self._callbacks[name] = cb

    def handle_client(self, skt, addr):

        if self._mode == TCPServer.ModeSerial:
            parser = QkDaemonAPISerialParser()
        else:
            parser = QkDaemonAPIJsonParser()

        parser.set_callback("api_parsed", self._callbacks["handle_api"])

        while 1:
            try:
                data = skt.recv(1024)
                if not data:
                    break
                parser.parse_data(data)
            except Exception, e:
                self.log.error(str(e))
                break


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


class QkDaemon(pigeon.server.PigeonServer):

    def __init__(self, apipath):
        super(QkDaemon, self).__init__()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self._conn_factory = ConnFactory()
        self._api = QkDaemonAPI(apipath, self)
        self._tcp_server_thread = None
        self._tcp_server = TCPServer(TCPServer.ModeSerial)
        self._tcp_server.set_callback("handle_api", self._handle_api)
        self._api_requests_queue = Queue.Queue()
        self._api_requests_thread = threading.Thread(target=self._api_handler)
        self._api_requests_thread.setDaemon(True)
        self.use_prefix("qk_")

    def api(self):
        return self._api

    def _handle_signal(self, sig, frame):
        #TODO stop all servers
        #IOLoop.current().add_callback(IOLoop.current().stop)
        pass

    def run(self, cli=False):
        self._api_requests_thread.start()
        super(QkDaemon, self).run(cli)
        self._api_requests_thread.join()

    def _api_handler(self):
        while self._request_to_quit is False:
            req = self._api_requests_queue.get()
            req_keys = req.keys()
            if "method" in req_keys and \
               "uri" in req_keys and \
               "params" in req_keys:
                self._api.run(req["method"], req["uri"], req["params"])

    def _handle_api(self, method, uri, params={}):
        req = {
            "method": method,
            "uri": uri,
            "params": params
        }
        print("REQUEST: %s" % req)
        self._api_requests_queue.put(req)

    def _handle_packet_received_from_conn(self, pkt):
        pkt_str = json.dumps(pkt)
        self.log.debug("Send packet to clients: %s" % pkt_str)
        self._tcp_server.send_to_all(pkt_str + "\r\n")


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
        self._api_requests_queue.put({"dum": "my"})
        self._api_requests_thread.join()
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

