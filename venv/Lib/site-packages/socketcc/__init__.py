import json
from threading import Timer, Thread
from typing import Callable, Any
from enum import Enum, auto
import websocket
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


# ISAUTHENTICATED,1
# PUBLISH,2
# REMOVETOKEN,3
# SETTOKEN,4
# EVENT,5
# ACKRECEIVE,6

class EventEnum(Enum):
    PUBLISH = auto()
    REMOVE_AUTH_TOKEN = auto()
    SET_AUTH_TOKEN = auto()
    EVENT_CID = auto()
    AUTHENTICATED = auto()
    GOT_ACK = auto()


class SocketCC:
    def __init__(self, url):
        self.map = {}
        self.map_ack = {}
        self.id = ""
        self.count = 0
        self.auth_token = None
        self.url = url
        self.acks = {}
        self.channels = []
        self.enable_reconnect = True
        self.reconnect_delay = 3
        self.ws = None
        self.on_connected = self.on_disconnected = self.on_connect_error = self.on_set_auth = self.on_auth = None

    # @staticmethod
    # def parse(dataobject, rid, cid, event):
    #     if event is not '':
    #         if event == "#publish":
    #             # print "publish got called"
    #             return 2
    #         elif event == "#removeAuthToken":
    #             # print "remove auth got called"
    #             return 3
    #         elif event == "#setAuthToken":
    #             # print "set authtoken called"
    #             return 4
    #         else:
    #             # print "event got called with cid"+cid
    #             return 5
    #     elif rid == 1:
    #         # print "is authenticated got called"
    #         return 1
    #     else:
    #         # print "got ack"
    #         return 6

    @staticmethod
    def parse2(rid, event) -> EventEnum:
        if event is not '':
            if event == "#publish":
                # print "publish got called"
                return EventEnum.PUBLISH
            elif event == "#removeAuthToken":
                # print "remove auth got called"
                return EventEnum.REMOVE_AUTH_TOKEN
            elif event == "#setAuthToken":
                # print "set authtoken called"
                return EventEnum.SET_AUTH_TOKEN
            else:
                # print "event got called with cid"+cid
                return EventEnum.EVENT_CID
        elif rid == 1:
            # print "is authenticated got called"
            return EventEnum.AUTHENTICATED
        else:
            # print "got ack"
            return EventEnum.GOT_ACK

    def on(self, key, func):
        self.map[key] = func

    def on_channel(self, key, func):
        self.map[key] = func

    def on_ack(self, key, func):
        self.map_ack[key] = func

    def execute(self, key, obj):
        if key in self.map:
            func = self.map[key]
            if func is not None:
                func(key, obj)

    def has_event_ack(self, key):
        return key in self.map_ack

    def execute_ack(self, key, obj, ack):
        if key in self.map_ack:
            func = self.map_ack[key]
            if func is not None:
                func(key, obj, ack)

    def emit(self, event, obj, ack=None):
        emit_obj = {"event": event, "data": obj}
        if ack:
            emit_obj['cid'] = self.get_and_increment()
        self.ws.send(json.dumps(emit_obj, sort_keys=True))
        if ack:
            self.acks[self.count] = [event, ack]
        # logging.info("Emit data is " + json.dumps(emit_obj, sort_keys=True))

    def sub(self, channel):
        self.ws.send(
            "{\"event\":\"#subscribe\",\"data\":{\"channel\":\"" + channel + "\"},\"cid\":" + str(
                self.get_and_increment()) + "}")

    def subscribe(self, channel, ack=None):
        obj = {"channel": channel}
        sub_obj = {"event": "#subscribe", "data": obj, "cid": self.get_and_increment()}
        self.ws.send(json.dumps(sub_obj, sort_keys=True))
        self.channels.append(channel)
        if ack:
            self.acks[self.count] = [channel, ack]

    def unsubscribe(self, channel, ack=None):
        sub_obj = {"event": "#unsubscribe", "data": channel, "cid": self.get_and_increment()}
        self.ws.send(json.dumps(sub_obj, sort_keys=True))
        self.channels.remove(channel)
        if ack:
            self.acks[self.count] = [channel, ack]

    def publish(self, channel, data, ack=None):
        obj = {"channel": channel, "data": data}
        pub_obj = {"event": "#publish", "data": obj, "cid": self.get_and_increment()}
        self.ws.send(json.dumps(pub_obj, sort_keys=True))
        if ack:
            self.acks[self.count] = [channel, ack]

    def subscribe_channels(self):
        # subscribing to all channels
        for x in self.channels:
            self.sub(x)

    def ack(self, cid):
        ws = self.ws

        def message_ack(error, data):
            ack_object = {"error": error, "data": data, "rid": cid}
            ws.send(json.dumps(ack_object, sort_keys=True))

        return message_ack

    class BlankDict(dict):
        def __missing__(self, key):
            return ''

    def on_message(self, ws, message):
        # print(f"Type:'{type(message)}'")
        # print(f"Message:'{message}'")
        if message == "#1":
            # print ("got ping sending pong")
            self.ws.send("#2")
        else:
            logging.info(message)
            main_obj = json.loads(message, object_hook=self.BlankDict)
            data_obj = main_obj["data"]
            rid = main_obj["rid"]
            cid = main_obj["cid"]
            event = main_obj["event"]

            result = self.parse2(rid, event)
            if result == EventEnum.AUTHENTICATED:
                if self.on_auth is not None:
                    self.id = data_obj["id"]
                    self.on_auth(self, data_obj["isAuthenticated"])
                self.subscribe_channels()
            elif result == EventEnum.PUBLISH:
                self.execute(data_obj["channel"], data_obj["data"])
                logging.info("publish got called")
            elif result == EventEnum.REMOVE_AUTH_TOKEN:
                self.auth_token = None
                logging.info("remove token got called")
            elif result == EventEnum.SET_AUTH_TOKEN:
                logging.info("set token got called")
                if self.on_set_auth is not None:
                    self.on_set_auth(self, data_obj["token"])
            elif result == EventEnum.EVENT_CID:
                logging.info("Event got called")
                if self.has_event_ack(event):
                    self.execute_ack(event, data_obj, self.ack(cid))
                else:
                    self.execute(event, data_obj)
            else:
                logging.info("Ack receive got called")
                if rid in self.acks:
                    tup = self.acks[rid]
                    if tup is not None:
                        ack = tup[1]
                        ack(tup[0], main_obj["error"], main_obj["data"])
                    else:
                        logging.info("Ack function not found for rid")

    def on_error(self, ws, error):
        if self.on_connect_error is not None:
            self.on_connect_error(self, error)
            # self.reconnect()

    def on_close(self, ws):
        if self.on_disconnected is not None:
            self.on_disconnected(self)
        if self.enable_reconnect:
            self.reconnect()

    def get_and_increment(self):
        self.count += 1
        return self.count

    def reset_count(self):
        self.count = 0

    def on_open(self, ws):
        self.reset_count()

        if self.on_connected is not None:
            self.on_connected(self)

        obj = {"authToken": self.auth_token}
        handshake_obj = {"event": "#handshake", "data": obj, "cid": self.get_and_increment()}
        self.ws.send(json.dumps(handshake_obj, sort_keys=True))

    def set_auth_token(self, token):
        self.auth_token = str(token)
        # print "Token is"+self.authToken

    def connect_thread(self, sslopt=None, http_proxy_host=None, http_proxy_port=None, enable_trace=False):
        t = Thread(target=self.connect, args=(sslopt, http_proxy_host, http_proxy_port, enable_trace))
        t.daemon = True
        t.start()

    def connect(self, sslopt=None, http_proxy_host=None, http_proxy_port=None, enable_trace=False):
        websocket.enableTrace(enable_trace)
        self.ws = websocket.WebSocketApp(self.url,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.run_forever(sslopt=sslopt, http_proxy_host=http_proxy_host, http_proxy_port=http_proxy_port)

    def set_basic_listener(self, connected_cb, disconnected_cb, connect_error_cb):
        self.on_connected = connected_cb
        self.on_disconnected = disconnected_cb
        self.on_connect_error = connect_error_cb

    def reconnect(self):
        Timer(self.reconnect_delay, self.connect).start()

    def set_delay(self, reconnect_delay):
        self.reconnect_delay = reconnect_delay

    def set_reconnection(self, enable):
        self.enable_reconnect = enable

    def set_auth_listener(self, set_auth_cb, on_auth_cb):
        self.on_set_auth = set_auth_cb
        self.on_auth = on_auth_cb

    def disconnect(self):
        self.enable_reconnect = False
        self.ws.close()

    emit_ack = emit
    subscribe_ack = subscribe
    publish_ack = publish
    unsubscribe_ack = unsubscribe
