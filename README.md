## Objective:
End to end performance testing and automated optimization for both Apache Kafka and clients.

## Problem: 
The performance testing tool that ships with Apache Kafka only tests publish latency/throughput leaving consumer performance untested.  Additionally, the performance test does not attempt to optimize Kafka or the clients.

## Quickstart:
This script uses the python client and Traffic Control (tc) to optionally simulate network latency, so you'll need to run as root.

Tested on Ubuntu 22.04

1. apt-get update
2. apt install python3.10-venv openjdk-21-jdk
3. python3 -m venv myenv
4. source myenv/bin/activate
5. pip install confluent-kafka
6. python gen_json.py (generate your test data)
7. python test-runner.py (run the test)

## What the script does:
Repeatedly installs and tests throughput up to maximum end to end latency, records results, updates settings, and repeats.  If any settings increase throughput, the setting will be adopted as the new default for the rest of the test.  At the end of the test, the best configuration of Kafka and the clients will be output.

In case we want to run a test with existing Kakfa or clients, starting the Kafka application and running an end to end producer/consumer test can both be run individually.

## Limitations and caveats:
Even with NTP, time between different servers can be off by several milliseconds.  By using the same server as the producer and consumer we can be sure that end to end latency reports are correct.  

For especially large implementations of Kafka, you will need to run the client test separately and across multiple servers and aggregate the test results together.

ZGC is not tested.  In my experience Kafka doesn't need large heaps, but ZGC does look promising for reliably low latency Kafka usage.

This script does not test updating OS settings like huge pages. That could be added at a later date.  This is what I'm using, but your implementation may need different settings.

/etc/sysctl.conf:
```bash
fs.file-max = 100000
vm.max_map_count = 262144
net.core.wmem_max = 16777216
net.core.rmem_max = 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_rmem = 4096 65536 16777216
vm.swappiness = 1
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_keepalive_intvl = 10
net.core.somaxconn = 65535
vm.nr_hugepages = 24576
fs.aio-max-nr = 1048576
```

/etc/security/limits.conf:
```bash
* hard nofile 100000
* soft nproc 65536
* hard nproc 65536
* soft core 0
* hard core unlimited
* soft memlock unlimited
* hard memlock unlimited
```

XFS file system
mq-deadline IO scheduler

JMX monitoring is not configured for the network namespace.  Could be added at a future date.

## Usage
Edit the "test-runner.py" script to choose what configs you want to optimize and what values you want to test for.

For example, the following would only test and output results from "batch_size" in the "LATENCY_PARAMETERS" section:

```bash
KAFKA_PARAMS_TO_TEST = []
LATENCY_PARAMS_TO_TEST = ['batch_size']
```

The following would test all said parameters for both the Kafka server and Kafka client.
```bash
KAFKA_PARAMS_TO_TEST = ['KAFKA_HEAP_OPTS', 'MaxGCPauseMillis', 'KAFKA_NUM_NETWORK_THREADS', 'KAFKA_NUM_IO_THREADS', 'socket_send_buffer_bytes', 'socket_receive_buffer_bytes']
LATENCY_PARAMS_TO_TEST = ['consumer_count', 'partitions', 'producer_count', 'fetch_wait_max_ms']
```

You may want to run the test a few times, each time updating the "defaults" sections with the best results just in case an earlier value tested could be changed after a later value was changed.  For example, if the batch size is very high, a higher socket send and receive buffer would help.  Here is where you set the defaults:
```bash
# Set initial defaults
defaults = {
    'KAFKA_HEAP_OPTS': "-Xms4G -Xmx4G",
    'MaxGCPauseMillis': "20",
    'G1ConcRefinementThreads': "16",
    'G1ParallelGCThreads': "16",
    'KAFKA_NUM_NETWORK_THREADS': "4",
    'KAFKA_NUM_IO_THREADS': "16",
    'socket_send_buffer_bytes': "1048576",
    'socket_receive_buffer_bytes': "524288",
    'socket_request_max_bytes': "104857600",
    'latency': "2ms"
}

# Set initial defaults for latency.py parameters
latency_defaults = {
    'batch_size': "16384",
    'compression_type': "snappy",
    'consumer_count': "5",
    'producer_count': "5",
    'fetch_wait_max_ms': "5",
    'partitions': "10",
    'bootstrap_server': "10.10.1.2:9092",
    'linger_ms': "50",
    'max_in_flight_messages': "5",
    'max_latency': "600",
    'acks': "-1",
    'num_messages': "6400"
}
```

You can edit the topic name, warmup time, and runtime of the test under the "run_test" function definition.  You may want to run the test for an extended period of time to make sure an old collection is included in the maximum latency if you have strict requirements.

```bash
        '--topic', 'test_topic',
        '--runtime', '20',
        '--warmup-time', '30',
```

The results from your test will be written to the "test_results" directory.

If you want to manually test Kafka or client options, you can run the "setup_kafka.py" and "latency.py" scripts individually.
```bash
python setup_kafka.py --help

```
```bash
python latency.py --help

```
## Known issues:
During some failure conditions the virtual interface is not deleted.  Delete with 'ip link delete veth0'.


