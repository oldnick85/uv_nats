# UV_NATS

This program is written to test the joint work of the libraries
[nats.c](https://github.com/nats-io/nats.c) and [libuv](https://github.com/libuv/libuv).

The program starts two threads with two libuv loops, one of which works with UDP packets, the other with NATS messages.
Since nats.c architecturally transmits messages in its threads, messages are passed to the libuv loop through a thread-safe queue.

It is assumed that the program will be launched in two copies, one of which is in master mode.
After launching, each program creates a UDP socket, the address of which is exchanged with the other instance via NATS.
As soon as the address of the remote UDP socket is received, the exchange of UDP packets and NATS messages begins.

UDP packet exchange consists of sending packets of a certain size according to a timer, as well as receiving and recording statistics of all incoming and outgoing packets.

NATS messages are exchanged in the ping-pong format. The program sends a ping message and waits for a response.
If a ping message is received, a pong message is immediately sent in response.
Request-response time statistics are kept, as well as statistics of all incoming and outgoing messages.

## Ubuntu 24.04+ build instructions

### Preparing for build

```bash
python3 -m venv env
source env/bin/activate
pip install conan
conan profile detect --force
conan install . --output-folder=build --build=missing
```

### Build

```bash
cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake
cmake --build .
```

## Run example

There are 3 terminals need to run.

One run the NATS server in a docker container, the other two run two instances of the program.

Running the NATS server:
```bash
docker run -it --rm --name nats-server -p 4222:4222 -p 8222:8222 nats:latest
```
add -D or -DV if needed to see detailed logs.

Starting programs

Master:
```bash
./uv_nats --work-time=300 --udp-repeat=10 --nats-repeat=50 --udp-payload=1000 --nats-payload=200 --master
```
and non-master:
```bash
./uv_nats --work-time=300 --udp-repeat=10 --nats-repeat=50 --udp-payload=1000 --nats-payload=200
```
The arguments mean that a UDP packet of size 1000 will be transmitted every 10 milliseconds,
and NATS ping messages will be transmitted every 50 milliseconds and a payload of 200 bytes will be added to their body.

![Work example](doc/example.png)