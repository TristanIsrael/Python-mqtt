"""
    \author Tristan Israël (tristan.israel@alefbet.net)

    Creates one or more tunnels between MQTT messaging sockets (application) and an MQTT broker.
    All sockets are Unix Domain sockets.

    Standalone
    ==========

    As a standalone python script by providing the following arguments :
    - (mandatory) a path containing broker sockets
    - (mandatory) a path containing messagging sockets
    - (optional) a filter for the messaging sockets file nanmes (default is '*').

    On start the script begins monitoring the messaging sockets path. Each time a new file
    matching the filter is found a new tunnel is created.

    When a new tunnel is created it consumes one of the broker sockets. The limit of tunnels is
    the number of broker sockets in the path.

    Library
    =======

    In a library the class :class:`UnixSocketTunneler` can be used to create a new tunnel between
    two messaging sockets.

    Performance
    ===========

    A thread is created for each tunnel. When the broker or the client closes the MQTT connection 
    the tunnel and its thread are destroyed.
"""

import subprocess
import time
import threading
import select
import socket
import argparse

BROKER_SOCKETS_PATH = "/var/run"
MESSAGING_SOCKETS_PATH = "/tmp"
MESSAGING_SOCKETS_FILTER = "*"

BUFFER_SIZE = 4096
DEBUG = False
DEBUG_HEX = True

def hexdump(data, prefix=""):
    """ @brief Print binary data in hexdump format. """
    

    for i in range(0, len(data), 16):
        chunk = data[i:i + 16]
        hex_bytes = " ".join(f"{byte:02x}" for byte in chunk)
        ascii_bytes = "".join(chr(byte) if 32 <= byte <= 126 else '.' for byte in chunk)
        print(f"{prefix} {i:04x}: {hex_bytes:<48} {ascii_bytes}")

    print("EOF")

class UnixSocketTunneler:
    ''' @brief Creates a tunnel between two UNIX sockets 
    
        The tunnel reads data coming from both sides and writes them immediately
        on the other side.

        This tunneling facility has been designed in order to connect a UNIX serial socket 
        created by Mosquitto and a UNIX serial socket created by QEMU for a DomU.
    '''

    def __init__(self, client_socket_path, broker_socket_path, n_socket:int):
        self.client_socket_path = client_socket_path
        self.broker_socket_path = broker_socket_path
        self.n_socket = n_socket

    def tunnel(self, messaging_socket_path:str):
        ''' @brief Setup connections and manage bidirectional tunneling.
        
        This function creates a new socket and waits for a connection on the local side. The
        connection to the remote is made only when the local side is connected.
        '''
        client_to_broker_buffer = b''
        broker_to_client_buffer = b''

        while True:
            # The tunnel is re-created as much as necessary to stay alive

            try:
                # Create broker socket
                broker_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

                # Connect to the client and wait for data
                client_sock = self.__connect_to_client_socket_and_wait_for_data()

                broker_socket_path = f"/tmp/mqtt_msg_{self.n_socket}.sock"
                broker_sock.connect(broker_socket_path)
                
                while True:
                    rlist = []
                    wlist = []

                    # Watch broker and client socket for incoming data
                    rlist.append(client_sock)
                    rlist.append(broker_sock)

                    # Write only when there are data to be sent
                    if client_to_broker_buffer:
                        wlist.append(broker_sock)
                    if broker_to_client_buffer:
                        wlist.append(client_sock)

                    readable, writable, _ = select.select(rlist, wlist, [])

                    # If the client sent data
                    if client_sock in readable:
                        try:
                            data = client_sock.recv(4096)
                            if DEBUG:
                                print(f"received {len(data)} bytes from client")

                            if data:
                                client_to_broker_buffer += data

                                if DEBUG:
                                    self.__debug_data(data, messaging_socket_path, "proxy")
                            else:
                                print(f"Client socket closed on {messaging_socket_path}.")
                                break
                        except BlockingIOError:
                            pass

                    # If the broker sent data
                    if broker_sock in readable:
                        try:
                            data = broker_sock.recv(4096)
                            if DEBUG:
                                print(f"received {len(data)} bytes from broker")

                            if data:
                                broker_to_client_buffer += data
                                if DEBUG:
                                    self.__debug_data(data, broker_socket_path, "proxy")
                            else:
                                print(f"Broker socket closed on {broker_socket_path}.")
                                break
                        except BlockingIOError:
                            pass

                    # Send to the broker
                    if broker_sock in writable and client_to_broker_buffer:
                        try:
                            sent = broker_sock.send(client_to_broker_buffer)
                            
                            if DEBUG:
                                print(f"sent {sent} to broker")
                                self.__debug_data(client_to_broker_buffer, "proxy", broker_socket_path)

                            client_to_broker_buffer = client_to_broker_buffer[sent:]
                        except BlockingIOError:
                            pass

                    # Send to the client
                    if client_sock in writable and broker_to_client_buffer:
                        try:
                            sent = client_sock.send(broker_to_client_buffer)
                            
                            if DEBUG:
                                print(f"sent {sent} to client")
                                self.__debug_data(client_to_broker_buffer, "proxy", messaging_socket_path)

                            broker_to_client_buffer = broker_to_client_buffer[sent:]
                        except BlockingIOError:
                            pass
                    
            except socket.error as e:
                print(f"{self.client_socket_path} --> {self.broker_socket_path}. Socket error: {e}.")
            finally:
                client_sock.close()
                broker_sock.close()
                client_to_broker_buffer = b''
                broker_to_client_buffer = b''
                time.sleep(1)  # Wait before retrying

    def __connect_to_client_socket_and_wait_for_data(self) -> socket.socket:
        ''' @brief Connects to the messaging socket and waits for data.
        
            This function is blocking until a connection is made on the new socket.
        '''
        client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client_sock.connect(self.client_socket_path)

        print(f"Connected to {self.client_socket_path} and waiting for data")

        ready_to_read, _, _ = select.select([client_sock], [], [])
        if client_sock in ready_to_read:
            print("Client has sent its first byte.")
            return client_sock
        
    def __debug_data(self, data, emitter, receiver):
        ''' Writes debugging information about the sockets communication '''

        if DEBUG_HEX:
            hexdump(data, f"from {emitter} to {receiver}")
        else:
            print(f"from {emitter} to {receiver}: {data}")


def create_msg_tunnel(client_socket:str, n_socket:int):
    ''' Creates a new tunnel between two messaging sockets
    '''

    print(f"Creating new tunnel with client socket {client_socket} with ID {n_socket}.")
    tunneler = UnixSocketTunneler(client_socket, BROKER_SOCKETS_PATH, n_socket)
    tunneler.tunnel(client_socket)


def wait_for_broker_socket() -> bool:
    ''' Waits for the MQTT broker to create its sockets '''

    print("Waiting for MQTT Broker sockets...")
    
    cmd = f"find /var/run/ -name '{BROKER_SOCKETS_PATH}'"

    while True:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        files = result.stdout.strip().split("\n")
        
        if len(files) == 0:
            time.sleep(1)
        else:
            print("Broker sockets ready")
            return True


def watch_msg_sockets():
    ''' Monitors DomU sockets and starts a tunnel. 
    
        This functions looks for messaging sockets in the /var/run folder. When a new
        socket appears, a tunnel is created with the next available socket for Mosquitto.
    '''
    print("Looking for msg mqtt sockets")

    cmd = f"find {MESSAGING_SOCKETS_PATH} -name '{MESSAGING_SOCKETS_FILTER}'"
    sockets = set()
    n_socket = 1

    while True:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        files = result.stdout.strip().split("\n")
        
        new_files = set(files) - sockets
        for file in new_files:
            if file == "":
                continue
            print(f"New messaging socket found : {file}")
            threading.Thread(target=create_msg_tunnel, args=(file, n_socket)).start()
            n_socket += 1

        sockets.update(files)
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creates tunnels between messaging sockets and MQTT broker sockets")
    
    # Arguments obligatoires
    parser.add_argument("--broker_path", "-b", help="Broker sockets path is mandatory", required=True)
    parser.add_argument("--messaging_path", "-m", help="Messaging sockets path is mandatory", required=True)
    parser.add_argument("--messaging_filter", "-f", help="Messaging sockets filter (eg app_*.sock)", default="*")
    
    args = parser.parse_args()

    BROKER_SOCKETS_PATH = args.broker_path
    MESSAGING_SOCKETS_PATH = args.messaging_path
    MESSAGING_SOCKETS_FILTER = args.messaging_filter

    print("Starting MQTT tunnels")
    print(f"  Broker sockets path: {BROKER_SOCKETS_PATH}")
    print(f"  Messaging sockets path: {MESSAGING_SOCKETS_PATH}")
    print(f"  Messaging sockets filter: {MESSAGING_SOCKETS_FILTER}")

    wait_for_broker_socket()

    threading.Thread(target=watch_msg_sockets).start()
