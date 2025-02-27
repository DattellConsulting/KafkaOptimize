
## Objective:
End to end performance testing and automated optimization for both Apache Kafka and clients.

## Problem: 
The performance testing tool that ships with Apache Kafka tests publish latency/throughput leaving consumer performance untested.  Additionally, the performance test does not attempt to optimize Kafka or the clients.

## Quickstart:
This script uses the python client and Traffic Control (tc) to simulate network latency, so you'll need to run as root.

Tested on Ubuntu 22.04.

1. apt-get update
2. apt install python3.10-venv openjdk-21-jdk
3. python3 -m venv myenv
4. source myenv/bin/activate
5. pip install confluent-kafka PyYAML
6. python gen_json.py  # generate test data as large_flattened.json
7. mkdir /mnt/data/kafkatest # default mount point
8. python test-runner.py  # runs the test

## What the script does:
Installs and tests throughput up to maximum end to end latency, records results, updates settings, and repeats.  If any settings increase throughput, the setting will be adopted as the new default for the rest of the test.  At the end of the test, the best configuration of Kafka and the clients will be output.

In case we want to run a test with existing Kafka or clients, starting the Kafka application and running an end to end producer/consumer test can both be run individually.

## When should this script be used
* Discover hardware bottlenecks/optimal instance size and disk IOPS using the monitoring endpoints along with maximum throughput.
* Replace the large_flattened.json file with a copy of your data, get optimized settings for both your server and clients.
* Use the csv output to generate graphs or other data to show examples of how different settings and hardware affect throughput.
* For training new users and admins, discover how individual settings affect the cluster.
* For integration testing, consider using setup_kafka.py to create kafka in your test environment.


## Limitations and caveats:
Some Kafka setting will interact with each other in negative ways so be mindful of the default settings set in config.yaml.  For example, if I am testing ten consumer processes with a single partition, nine of my consumers will be idle.

This script uses a single server and uses tc to simulate network latency. The automated "test-runner.py" tool will be effective at discovering the effect of specific configurations on both clients and the server.

For testing existing Kafka clusters, try running only the "latency.py" file and redirected the script towards your kafka implementation.  Conversely, if you want to test your local client against Kafka, try running only the "setup_kafka.py" script to setup a local version of kafka with simulated latency.

ZGC is not included in the test.  Support can be added later if there is a need.

tc supports packet loss and network latency jitter, however these are not implemented. 

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

XFS file system, mq-deadline IO scheduler


## Usage
Edit the "config.yaml" to choose what configs you want to optimize, and what values you want to test for, and what the starting default settings are.

Starting default settings and what values to test for can matter significantly.  For example, "socket_receive_buffer_bytes" will affect how large of a batch can be successfully sent through.

You can edit the topic name, warmup time, and runtime of the test under the "run_test" function definition within "test-runner.py".  You may want to run the test for an extended period of time to make sure an old collection is included in the maximum latency if you have strict requirements.

```bash
        '--topic', 'test_topic',
        '--runtime', '20',
        '--warmup-time', '30',
```

You can edit the data directory inside the "test-runner.py" file here:

```bash
'--data-directory', '/mnt/data/kafkatest'
```

Tesults from each test will be written to the "test_results" directory as a csv file.  The following information is recorded: Test Name,Max Latency,Avg Latency,P99 Latency,Messages Per Second,Kafka Parameters,Client Parameters

If you want to manually test Kafka or client options, you can run the "setup_kafka.py" and "latency.py" scripts individually.
```bash
python3 setup_kafka.py --help
usage: setup_kafka.py [-h] [--namespace NAMESPACE] [--veth0 VETH0] [--veth1 VETH1] [--host_ip HOST_IP] [--ns_ip NS_IP] [--latency LATENCY]
                      --data-directory DATA_DIRECTORY [--KAFKA_HEAP_OPTS KAFKA_HEAP_OPTS] [--MaxGCPauseMillis MAXGCPAUSEMILLIS]
                      [--G1ConcRefinementThreads G1CONCREFINEMENTTHREADS] [--G1ParallelGCThreads G1PARALLELGCTHREADS]
                      [--KAFKA_NUM_NETWORK_THREADS KAFKA_NUM_NETWORK_THREADS] [--KAFKA_NUM_IO_THREADS KAFKA_NUM_IO_THREADS]
                      [--KAFKA_NUM_PARTITIONS KAFKA_NUM_PARTITIONS] [--kafka_port KAFKA_PORT]
                      [--socket-send-buffer-bytes SOCKET_SEND_BUFFER_BYTES] [--socket-receive-buffer-bytes SOCKET_RECEIVE_BUFFER_BYTES]
                      [--socket-request-max-bytes SOCKET_REQUEST_MAX_BYTES]
```
```bash
python3 latency.py --help
usage: latency.py [-h] [--bootstrap-server BOOTSTRAP_SERVER] [--topic-name TOPIC_NAME] [--num-messages NUM_MESSAGES]
                  [--warmup-time WARMUP_TIME] [--runtime RUNTIME] [--partitions PARTITIONS] [--replication-factor REPLICATION_FACTOR]
                  [--batch-size BATCH_SIZE] [--acks ACKS] [--producer-count PRODUCER_COUNT] [--consumer-count CONSUMER_COUNT]
                  [--fetch-wait-max-ms FETCH_WAIT_MAX_MS] [--max-latency MAX_LATENCY] [--max-in-flight-messages MAX_IN_FLIGHT_MESSAGES]
                  [--compression-type COMPRESSION_TYPE] [--linger-ms LINGER_MS]
                  [--queue-buffering-max-messages QUEUE_BUFFERING_MAX_MESSAGES] --payload-file PAYLOAD_FILE
```

## Monitoring
JMX is available by default on 10.10.1.2:9999.  Kafka stats available on 10.10.1.2:9092.  This is the virtual interface that creates artificial latency for Kafka --  note that monitoring will get the same latency. I prefer DataDog for internet connected implementations and OpenSearch for air gapped implementations.

## Known issues:
During some failure conditions the virtual interface is not deleted.  Delete with 'ip link delete veth0'.
Test only supports plaintext.

