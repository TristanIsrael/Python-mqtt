# Python mqtt

Repository with python MQTT client class based on Paho mqtt with serial port capabilities and a proxy class between MQTT unix sockets

These classes come from [Safecor](https://github.com/TristanIsrael/Safecor), the operational system for security products.

## Classes

- UnixSocketTunneler can be used to create a tunnel between two UNIX domain sockets that copies data in both ways.
- MqttClient - A generic MQTT client based on paho MQTT. It can handle connections to serial ports, unix domain sockets and TCP ports.
- SerialMqttClient - The serial port facility for MqttClient.

## Licence

This code is provided with the GPLv3 licence and free to use. No support is provided but improvements and bug fixes are welcome.
