""" \author Tristan Israël (tristan.israel@alefbet.net) """

from enum import StrEnum
import json
import threading
import select
import traceback
from typing import Literal, Callable, Optional
try:
    import serial
except ImportError:
    pass
import paho.mqtt.client as mqtt
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode

DEBUG = False
TYPE_CHECKING = True

class ConnectionType(StrEnum):
    ''' @brief This enumeration identifies the type of connection to the broker
    '''
    UNIX_SOCKET = "unix_socket"
    SERIAL_PORT = "serial_port"
    TCP_DEBUG = "tcp"

class SerialSocket():
    ''' @brief This class implements a serial socket to an MQTT broker

    The serial socket is used to communicate on a serial port or a Unix domain socket
    '''

    def __init__(self, path:str, baudrate:int):
        if DEBUG:
            print(f"Connect to serial port {path}")
        self.serial = serial.Serial(port=path, baudrate=baudrate, timeout=1.0, write_timeout=0)
        self.serial.reset_input_buffer()
        self.serial.reset_output_buffer()


    def recv(self, buffer_size: int) -> bytes:
        #print(f"Wants to read {buffer_size} bytes")
        if self.serial is not None and self.serial.is_open:
            data = self.serial.read(buffer_size)
            #print(f"Read {len(data)} bytes")
            return data

        return b''

    def in_waiting(self) -> int:
        return self.serial.in_waiting

    def pending(self) -> int:
        if self.serial is not None:
            return self.serial.out_waiting
        
        return 0

    def send(self, buffer: bytes) -> int:
        if self.serial is not None and self.serial.is_open:
            #print("send:{}".format(buffer))
            sent = self.serial.write(buffer)
            return sent if sent is not None else 0
        
        return 0

    def close(self) -> None:     
        if self.serial is not None and self.serial.is_open:
            self.serial.reset_input_buffer()
            self.serial.reset_output_buffer()
            self.serial.close()

    def fileno(self) -> int:
        return self.serial.fileno()

    def setblocking(self, flag: bool) -> None:
        pass


class SerialMQTTClient(mqtt.Client):
    """ This class is used by the MQTT client to communicate on a serial port """    

    on_connection_lost: Optional[Callable[[], None]] = None

    def __init__(self, path:str, baudrate:int, *args, **kwargs):
        super().__init__(callback_api_version=CallbackAPIVersion.VERSION2, *args, **kwargs)
        self.path = path
        self.baudrate = baudrate
        self._sock = None

    def disconnect(self, reasoncode: ReasonCode | None = None, properties: Properties | None = None):
        self._send_disconnect(reasoncode, properties)
        self.__do_loop()

    def close(self):
        self.disconnect()
        self._sock_close()

    def loop_start(self) -> MQTTErrorCode:
        self._thread_terminate = False
        self._thread = threading.Thread(target=self.__do_loop)
        self._thread.daemon = True
        self._thread.start()

        return MQTTErrorCode.MQTT_ERR_SUCCESS

    def __do_loop(self):
        rc = MQTTErrorCode.MQTT_ERR_SUCCESS
        timeout = 1.0

        while not self._thread_terminate:
            if self._sock is None:
                print("No socket found, exiting loop")
                if self.on_connection_lost is not None:
                    self.on_connection_lost()
                return
            
            # if bytes are pending do not wait in select
            pending_bytes = self._sock.pending()
            if pending_bytes > 0:
                timeout = 0.0

            rlist, _, _ = select.select([self._sock], [], [], timeout)

            if rlist and self._sock.in_waiting() > 0:
                rc = self.loop_read()
                if rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
                    print(f"Read error {rc}")
                    break

            if self.want_write():
                rc = self.loop_write()
                if rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
                    print(f"Write error {rc}")
                    break

            rc = self.loop_misc()
            if rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
                print(f"Misc error {rc}")
                if rc == MQTTErrorCode.MQTT_ERR_CONN_LOST:
                    print("Connection lost.")
                    if self.on_connection_lost is not None:
                        self.on_connection_lost()

                break

            #time.sleep(0.2)

        print("MQTT loop ended")

    def loop_stop(self) -> MQTTErrorCode:
        self._thread_terminate = True

        if self._thread is not None:
            self._thread.join()
            return MQTTErrorCode.MQTT_ERR_SUCCESS

        return MQTTErrorCode.MQTT_ERR_UNKNOWN

    def _create_socket(self):
        try:
            print(f"Create socket on {self.path}")
            self._sock = SerialSocket(self.path, self.baudrate)
            self._sockpairR = self._sock
            return self._sock
        except Exception as e:
            print("An error occured while opening the serial port")
            print(e)
            return None

class MqttClient():
    """ This class is a client to an MQTT broker 
    
    It can connect to a broker on a serial port, a unix socket or a TCP network port.
    """

    connection_type:ConnectionType = ConnectionType.UNIX_SOCKET
    connection_string:str = ""
    identifier:str = "unknown"
    on_connected: Optional[Callable[[], None]] = None
    on_message: Optional[Callable[[str, dict], None]] = None
    on_subscribed: Optional[Callable[[], None]] = None
    on_log: Optional[Callable[[int, str], None]] = None
    __message_callbacks = []
    __connected_callbacks = []
    connected = False
    is_starting = False
    __debugging = False

    def __init__(self, identifier:str, connection_type:ConnectionType = ConnectionType.UNIX_SOCKET, connection_string:str = "", debugging = False):
        """
        Configures a connection to an MQTT broker
                
        :param identifier: The client identifier on the MQTT broker
        :type identifier: str
        :param connection_type: The connection type (serial, unix domain socket or TCP)
        :type connection_type: ConnectionType
        :param connection_string: The connection string to the MQTT broker (see paho.mqtt documentation for more)
        :type connection_string: str
        :param debugging: if True, debug messages will be added to stdout
        """
        self.mqtt_client = None
        self.identifier = identifier
        self.connection_type = connection_type
        self.connection_string = connection_string
        self.__debugging = debugging
        self.__subscriptions = []

    def __del__(self):
        self.stop()

    def start(self):
        """ Tries to connect to the MQTT broker if not already connected """
        if self.is_starting or self.connected:
            return
        
        self.is_starting = True
        print(f"Starting MQTT client {self.identifier}")

        if self.connection_type != ConnectionType.SERIAL_PORT:
            self.mqtt_client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION2,
                client_id=self.identifier,
                transport=self.__get_transport_type(),
                reconnect_on_failure=True
            )
            self.mqtt_client.on_connect = self.__on_connected
            self.mqtt_client.on_message = self.__on_message
            self.mqtt_client.on_subscribe = self.__on_subscribe
            self.mqtt_client.on_disconnect = self.__on_disconnected
            if self.__debugging:
                self.mqtt_client.on_log = self.__on_log

            mqtt_host = "undefined"
            try:
                if self.connection_type == ConnectionType.TCP_DEBUG:
                    mqtt_host = "localhost"
                    self.mqtt_client.connect(host=mqtt_host, keepalive=30)
                elif self.connection_type == ConnectionType.UNIX_SOCKET:
                    mqtt_host = self.connection_string
                    self.mqtt_client.connect(host=mqtt_host, port=1, keepalive=30)
                else:
                    print(f"The connection type {self.connection_type} is not handled")
                    return
            except Exception as e:
                print(f"Could not connect to the MQTT broker on {mqtt_host}")
                print(e)
                return
            
            self.mqtt_client.loop_start()
        elif self.connection_type == ConnectionType.SERIAL_PORT:
            self.mqtt_client = SerialMQTTClient(
                client_id=self.identifier,
                path=self.connection_string,
                baudrate=115200,
                reconnect_on_failure=True
            )
            self.mqtt_client.on_connect = self.__on_connected
            self.mqtt_client.on_message = self.__on_message
            self.mqtt_client.on_disconnect = self.__on_disconnected
            self.mqtt_client.on_subscribe = self.__on_subscribe
            self.mqtt_client.on_connection_lost = self.__on_connection_lost
            if self.__debugging:
                self.mqtt_client.on_log = self.__on_log

            if DEBUG:
                self.mqtt_client.on_log = self.__on_log
            self.mqtt_client.connect(host="localhost", port=1, keepalive=5)

            self.mqtt_client.loop_start()
        else:
            print(f"The connection type {self.connection_type} is not handled")
            return        

    def add_connected_callback(self, callback):
        """ Adds a callback function that will be called when the connection is made to the broker 
        
        The callback function has no argument
        """

        self.__connected_callbacks.append(callback)

    def add_message_callback(self, callback):
        """ Adds a callback function that will be called when a message is received by the client 
        
        The callback function has the following arguments:
          topic:str - The topic of the message
          payload:dict - The payload of the message
        """
        self.__message_callbacks.append(callback)

    def del_message_callback(self, callback):
        """ Removes a message callback """

        self.__message_callbacks.remove(callback)

    def reset_message_callbacks(self):
        """ Removes all messages callbacks """
        self.__message_callbacks.clear()

    def stop(self):
        """ Stops the MQTT client and disconnects from the broker """
        if self.mqtt_client is not None:
            #print("Quit Mqtt client")
            try:
                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
                if self.connection_type == ConnectionType.SERIAL_PORT:
                    self.mqtt_client.close()
            except:
                #Ignore exceptions when closing
                pass
            finally:
                self.connected = False
                self.is_starting = False

    def subscribe(self, topic:str) -> tuple[MQTTErrorCode, int | None]:
        """ Subscribe to a topic on the broker """

        print(f"Subscribed to {topic}")
        self.__subscriptions.append(topic)
        return self.mqtt_client.subscribe(topic)

    def unsubscribe(self, topic:str):
        """ Unsubscribes from a specific topic on the broker """

        print(f"Unsubscribed from {topic}")
        self.__subscriptions.remove(topic)
        return self.mqtt_client.unsubscribe(topic)

    def unsubscribe_all(self):
        """ Unsubscribes from all topics on the broker """

        for topic in self.__subscriptions:
            self.unsubscribe(topic)

    def publish(self, topic:str, payload:dict):
        """ Sends a new message """
        
        data = json.dumps(payload)
        #print(data)
        #print(len(data))
        self.mqtt_client.publish(topic=topic, payload=data)

    def __get_transport_type(self) -> Literal['tcp', 'unix']:
        if self.connection_type == ConnectionType.TCP_DEBUG:
            return "tcp"
        else:
            return "unix"

    def __on_log(self, client, userdata, level, buf):
        if self.on_log is not None:
            self.on_log(level, buf)

    def __on_message(self, client:mqtt.Client, userdata, msg:mqtt.MQTTMessage):
        #print(f"[{userdata}] Message reçu sur {msg.topic}: {msg.payload.decode()}")        
        try:
            payload = json.loads(msg.payload.decode())
            if self.on_message is not None:
                self.on_message(msg.topic, payload)

            for cb in self.__message_callbacks:
                cb(msg.topic, payload)
        except Exception as e:
            print("[MQTT Client] Uncaught Exception when handling message:")
            print(f"Topic : {msg.topic}")
            print(f"Payload : {msg.payload}")
            print(f"Exception : {e}")
            print("** Notice that the error may come from the client callback **")

    def __on_connected(self, client:mqtt.Client, userdata, connect_flags, reason_code, properties):
        print("Connected to the MQTT broker")
        self.connected = True
        self.is_starting = False
        
        if self.on_connected is not None:
            self.on_connected()

        for cb in self.__connected_callbacks:
            cb()

    def __on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        #print("Subscribed to a topic")

        if self.on_subscribed is not None:
            self.on_subscribed(mid)

    def __on_connection_lost(self):
        print("Connection lost, trying to reconnect.")
        self.is_starting = False
        self.connected = False
        self.start()

    def __on_disconnected(self, *args):
        print("Disconnected from the broker")
        #print("Arguments:")
        #for arg in args:
        #    print(arg)

        #print("Disconnect from the broker")
        #self.mqtt_client.close()
        