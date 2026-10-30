""" MQTT client module.

This module provides the MQTT transports (UNIX domain socket, serial port,
TCP on localhost for debugging) and the :class:`MqttClient` used to
exchange messages over MQTT.

\author Tristan Israël
"""

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
from . import SysLogger

DEBUG = False
TYPE_CHECKING = True

class ConnectionType(StrEnum):
    ''' @brief This enumeration identifies the type of connection used.

    UNIX_SOCKET: the broker is reached through a UNIX domain socket.
    SERIAL_PORT: the broker is reached through a serial port.
    TCP_DEBUG: the broker is reached over TCP on localhost (development only).
    '''
    UNIX_SOCKET = "unix_socket"
    SERIAL_PORT = "serial_port"
    TCP_DEBUG = "tcp"

class SerialSocket():
    ''' @brief This class implements a serial socket.

    The serial socket is used to communicate over a serial port.
    '''

    def __init__(self, path:str, baudrate:int):
        """Opens the serial port and resets its input and output buffers.

        Args:
            path(str): The path of the serial port device.
            baudrate(int): The baudrate of the serial port.
        """
        self.serial = serial.Serial(port=path, baudrate=baudrate, timeout=1.0, write_timeout=0)
        self.serial.reset_input_buffer()
        self.serial.reset_output_buffer()


    def recv(self, buffer_size: int) -> bytes:
        """Reads up to buffer_size bytes from the serial port.

        Args:
            buffer_size(int): The maximum number of bytes to read.

        Returns:
            bytes: The bytes read, or an empty byte string if the port is not open.
        """
        #print(f"Wants to read {buffer_size} bytes")
        if self.serial is not None and self.serial.is_open:
            data = self.serial.read(buffer_size)
            #print(f"Read {len(data)} bytes")
            return data

        return b''

    def in_waiting(self) -> int:
        """Returns the number of bytes available to read on the serial port."""
        return self.serial.in_waiting

    def pending(self) -> int:
        """Returns the number of bytes pending to be written on the serial port.

        Returns:
            int: The number of pending bytes, or 0 if the port is not open.
        """
        if self.serial is not None:
            return self.serial.out_waiting
        
        return 0

    def send(self, buffer: bytes) -> int:
        """Writes the buffer to the serial port.

        Args:
            buffer(bytes): The bytes to write.

        Returns:
            int: The number of bytes written, or 0 if the port is not open.
        """
        if self.serial is not None and self.serial.is_open:
            #print("send:{}".format(buffer))
            sent = self.serial.write(buffer)
            return sent if sent is not None else 0
        
        return 0

    def close(self) -> None:
        """Resets the input and output buffers and closes the serial port."""
        traceback.print_stack()

        if self.serial is not None and self.serial.is_open:
            self.serial.reset_input_buffer()
            self.serial.reset_output_buffer()
            self.serial.close()

    def fileno(self) -> int:
        """Returns the file descriptor of the serial port (used by select)."""
        return self.serial.fileno()

    def setblocking(self, flag: bool) -> None:
        """Kept for compatibility with the socket interface: does nothing.

        Args:
            flag(bool): The blocking flag (ignored).
        """
        pass


class SerialMQTTClient(mqtt.Client):
    ''' @brief This class allows the MQTT client to communicate over a serial port.
    '''

    # Callback invoked when the network loop detects that the connection
    # was lost (no socket or a CONN_LOST error from the broker).
    on_connection_lost: Optional[Callable[[], None]] = None

    def __init__(self, path:str, baudrate:int, *args, **kwargs):
        """Creates a paho client whose network layer runs over a serial port.

        Args:
            path(str): The path of the serial port device.
            baudrate(int): The baudrate of the serial port.
            *args: Forwarded to the paho :class:`mqtt.Client` constructor.
            **kwargs: Forwarded to the paho :class:`mqtt.Client` constructor.
        """
        super().__init__(callback_api_version=CallbackAPIVersion.VERSION2, *args, **kwargs)
        self.path = path
        self.baudrate = baudrate
        self._sock = None

    def disconnect(self, reasoncode: ReasonCode | None = None, properties: Properties | None = None):
        """Sends the MQTT DISCONNECT packet and runs the network loop until closed."""
        self._send_disconnect(reasoncode, properties)
        self.__do_loop()

    def close(self):
        """Disconnects from the broker and closes the serial socket."""
        self.disconnect()
        self._sock_close()
        #if self.sock_ is not None:
        #    self.sock_.close()

    def loop_start(self) -> MQTTErrorCode:
        """Starts the network loop in a daemon thread (over the serial socket).

        Returns:
            MQTTErrorCode: MQTT_ERR_SUCCESS.
        """
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
                SysLogger("MQTT-client").warn("No socket found, exiting loop")
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
                    SysLogger("MQTT-client").warn(f"Read error {rc}")
                    break

            if self.want_write():
                rc = self.loop_write()
                if rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
                    SysLogger("MQTT-client").warn(f"Write error {rc}")
                    break

            rc = self.loop_misc()
            if rc != MQTTErrorCode.MQTT_ERR_SUCCESS:
                SysLogger("MQTT-client").warn(f"Misc error {rc}")
                if rc == MQTTErrorCode.MQTT_ERR_CONN_LOST:
                    SysLogger("MQTT-client").warn("Connection lost.")
                    if self.on_connection_lost is not None:
                        self.on_connection_lost()

                break

            #time.sleep(0.2)

    def loop_stop(self) -> MQTTErrorCode:
        """Stops the network loop thread and waits for it to terminate.

        Returns:
            MQTTErrorCode: MQTT_ERR_SUCCESS if the thread was stopped, MQTT_ERR_UNKNOWN otherwise.
        """
        self._thread_terminate = True

        if self._thread is not None:
            self._thread.join()
            return MQTTErrorCode.MQTT_ERR_SUCCESS

        return MQTTErrorCode.MQTT_ERR_UNKNOWN

    def _create_socket(self):
        """Opens the serial socket used by the paho network layer.

        Returns:
            SerialSocket: The opened socket, or None if it could not be opened.
        """
        try:
            self._sock = SerialSocket(self.path, self.baudrate)
            self._sockpairR = self._sock
            return self._sock
        except Exception as e:
            SysLogger("MQTT-client").warn("An error occured while opening the serial port")
            SysLogger("MQTT-client").warn(str(e))
            return None

class MqttClient():
    ''' @brief This class is used to exchange MQTT messages within the system.

    Attributes:
        connection_type(ConnectionType): The type of the connection to the broker.
        connection_string(str): The connection string (the UNIX socket path, the serial
            port path, or the TCP host depending on the connection type).
        identifier(str): The client identifier (used as the MQTT client ID).
        on_connected: Callback invoked when the client (re)connects to the broker.
        on_message: Callback invoked for every message received on a subscribed topic.
        on_subscribed: Callback invoked when the broker acknowledges a subscription.
        on_log: Callback invoked with the paho log messages (debugging only).
        connected(bool): True while the client is connected to the broker.
        is_starting(bool): True while the client is trying to connect.
    '''

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
        """Creates a MQTT client.

        Args:
            identifier(str): The client identifier (used as the MQTT client ID).
            connection_type(ConnectionType): The type of the connection to the broker.
            connection_string(str): The connection string (the UNIX socket path, the
                serial port path, or the TCP host depending on the connection type).
            debugging(bool): If True, the paho log messages are forwarded to on_log.
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
        """Creates the underlying paho client and connects it to the broker.

        The transport is selected by the connection type: a UNIX domain
        socket, a serial port, or TCP on localhost (debugging).
        No-op if the client is already connected or starting.
        """
        if self.is_starting or self.connected:
            return
        
        self.is_starting = True
        SysLogger("MQTT-client").info(f"Starting MQTT client {self.identifier}")

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
                    SysLogger("MQTT-client").error(f"The connection type {self.connection_type} is not handled")
                    return
            except Exception as e:
                SysLogger("MQTT-client").warn(f"Could not connect to the MQTT broker on {mqtt_host}")
                SysLogger("MQTT-client").warn(str(e))
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
            SysLogger("MQTT-client").error(f"The connection type {self.connection_type} is not handled")
            return        

    def add_connected_callback(self, callback):
        """Registers a callback invoked every time the client (re)connects to the broker.

        Args:
            callback: A callable with no argument.
        """
        self.__connected_callbacks.append(callback)

    def add_message_callback(self, callback):
        """Registers a callback invoked for every received message.

        Args:
            callback: A callable invoked with the topic (str) and the payload (dict).
        """
        self.__message_callbacks.append(callback)

    def del_message_callback(self, callback):
        """Removes a previously registered message callback.

        Args:
            callback: The callback to remove.
        """
        self.__message_callbacks.remove(callback)

    def reset_message_callbacks(self):
        """Removes all the registered message callbacks."""
        self.__message_callbacks.clear()

    def stop(self):
        """Disconnects from the broker and stops the network loop.

        Exceptions raised while closing the client are ignored.
        """
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
        """Subscribes to a topic on the broker. The topic is tracked by the client.

        Args:
            topic(str): The topic to subscribe to.

        Returns:
            tuple[MQTTErrorCode, int | None]: The paho result code and the message ID.
        """
        #print(f"Subscribed to {topic}")
        self.__subscriptions.append(topic)
        return self.mqtt_client.subscribe(topic)

    def unsubscribe(self, topic:str):
        """ Unsubscribes a client from a specific topic """

        #print(f"Unsubscribed to {topic}")
        self.__subscriptions.remove(topic)
        return self.mqtt_client.unsubscribe(topic)

    def unsubscribe_all(self):
        """ Unsubscribes a client from all topics """

        for topic in self.__subscriptions:
            self.unsubscribe(topic)

    def publish(self, topic:str, payload:dict):
        """Publishes a payload on a topic. The payload is serialized as JSON.

        Args:
            topic(str): The topic to publish to.
            payload(dict): The payload to publish.
        """
        data = json.dumps(payload, ensure_ascii=True)
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
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if self.on_message is not None:
                self.on_message(msg.topic, payload)

            for cb in self.__message_callbacks:
                cb(msg.topic, payload)
        except Exception as e:
            SysLogger("MQTT-client").warn("[MQTT Client] Uncaught Exception when handling message:")
            SysLogger("MQTT-client").warn(f"Topic : {msg.topic}")
            SysLogger("MQTT-client").warn(f"Payload : {msg.payload}")
            SysLogger("MQTT-client").warn(f"Exception : {e}")
            SysLogger("MQTT-client").warn("** Notice that the error may come from the client callback **")

    def __on_connected(self, client:mqtt.Client, userdata, connect_flags, reason_code, properties):
        SysLogger("MQTT-client").info("Connected to the MQTT broker")
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
        SysLogger("MQTT-client").warn("Connection lost, trying to reconnect.")
        self.is_starting = False
        self.connected = False
        self.start()

    def __on_disconnected(self, *args):
        SysLogger("MQTT-client").info("Disconnected from the broker")
        #print("Arguments:")
        #for arg in args:
        #    print(arg)

        #print("Disconnect from the broker")
        #self.mqtt_client.close()
