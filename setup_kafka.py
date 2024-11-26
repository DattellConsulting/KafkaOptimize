#!/usr/bin/env python3

import os
import subprocess
import argparse
import signal
import sys
import urllib.request
import tarfile
import time

# Global Kafka process handle
kafka_process = None

# Function to execute shell commands
def run_command(command):
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}\n{e}")
        sys.exit(1)

# Cleanup function to delete veth interfaces, namespace, and stop Kafka
def cleanup():
    print("Cleaning up...")
    if kafka_process:
        print("Stopping Kafka broker...")
        kafka_process.terminate()
        kafka_process.wait()
        print("Kafka broker stopped.")
        time.sleep(2)
    
    try:
        run_command(f"ip link set {args.veth0} down 2>/dev/null")
        run_command(f"ip link delete {args.veth0} 2>/dev/null")
        run_command(f"ip netns del {args.namespace} 2>/dev/null")
    except Exception as e:
        print(f"Error during cleanup: {e}")
    sys.exit(0)

# Check for sudo permissions
def check_sudo():
    if os.geteuid() != 0:
        print("This script must be run with sudo. Re-running with sudo...")
        try:
            os.execvp("sudo", ["sudo"] + sys.argv)
        except Exception as e:
            print(f"Failed to re-run the script with sudo: {e}")
            sys.exit(1)

# Handle exit signals to clean up on script termination
def signal_handler(sig, frame):
    cleanup()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Check for sudo permissions
check_sudo()

# Argument parsing for command line input
parser = argparse.ArgumentParser(description="Setup network namespace, latency, and start Apache Kafka.")
parser.add_argument('--namespace', type=str, default='myns', help='Name of the network namespace to create')
parser.add_argument('--veth0', type=str, default='veth0', help='Name of the veth interface on the host')
parser.add_argument('--veth1', type=str, default='veth1', help='Name of the peer veth interface')
parser.add_argument('--host_ip', type=str, default='10.10.1.1/24', help='IP address for the host side of the veth pair')
parser.add_argument('--ns_ip', type=str, default='10.10.1.2/24', help='IP address for the network namespace side of the veth pair')
parser.add_argument('--latency', type=str, default='5ms', help='Latency to apply on the veth interface')
parser.add_argument("--data-directory", required=True, help="Path to the Kafka data directory.")
parser.add_argument("--KAFKA_HEAP_OPTS",  type=str, default='-Xms2G -Xmx2G', help="Heap size settings for Kafka")
parser.add_argument("--MaxGCPauseMillis",  type=str, default='20', help="Maximum GC pause time.")
parser.add_argument("--G1ConcRefinementThreads",  type=str, default='8', help="Number of G1 concurrent refinement threads.")
parser.add_argument("--G1ParallelGCThreads",  type=str, default='8', help="Number of G1 Parallel GC threads.")
parser.add_argument("--KAFKA_NUM_NETWORK_THREADS",  type=str, default='4', help="Number of network threads for Kafka.")
parser.add_argument("--KAFKA_NUM_IO_THREADS",  type=str, default='6', help="Number of I/O threads for Kafka.")
parser.add_argument("--KAFKA_NUM_PARTITIONS",  type=str, default='10', help="Number of partitions for Kafka topics.")
parser.add_argument("--kafka_port", type=str, default='9092', help="Kafka port (default: 9092)")
parser.add_argument("--socket-send-buffer-bytes", type=int, default=102400, help="Kafka socket send buffer size (default: 102400)")
parser.add_argument("--socket-receive-buffer-bytes", type=int, default=102400, help="Kafka socket receive buffer size (default: 102400)")
parser.add_argument("--socket-request-max-bytes", type=int, default=104857600, help="Kafka socket request max bytes (default: 104857600)")

args = parser.parse_args()

# Function to check if the namespace exists
def namespace_exists(namespace):
    try:
        result = subprocess.run(f"ip netns list | grep {namespace}", shell=True, check=False, stdout=subprocess.PIPE)
        return result.stdout.strip() != b""
    except subprocess.CalledProcessError:
        return False

# Function to ensure Kafka storage formatting completes successfully
def initialize_kafka_cluster(kafka_dir, data_dir):
    print("Initializing Kafka cluster with KRaft...")
    original_cwd = os.getcwd()
    os.chdir(kafka_dir)
    try:
        kafka_storage = os.path.join("bin", "kafka-storage.sh")
        cluster_id_cmd = [kafka_storage, "random-uuid"]
        cluster_id = subprocess.check_output(cluster_id_cmd).decode().strip()
        print(f"Generated KAFKA_CLUSTER_ID: {cluster_id}")

        server_properties = os.path.join("config", "kraft", "server.properties")
        with open(server_properties, 'r') as file:
            properties = file.readlines()
        with open(server_properties, 'w') as file:
            for line in properties:
                if line.startswith("log.dirs"):
                    file.write(f"log.dirs={data_dir}\n")
                else:
                    file.write(line)

        format_cmd = [kafka_storage, "format", "-t", cluster_id, "-c", server_properties]
        result = subprocess.run(format_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error during Kafka storage formatting: {result.stderr}")
            sys.exit(1)
        else:
            print("Kafka storage formatted successfully.")
            print(result.stdout)
    finally:
        os.chdir(original_cwd)

# Function to download Kafka if not present
def download_kafka(kafka_url, kafka_dir):
    print("Downloading Apache Kafka...")
    file_name = kafka_url.split('/')[-1]
    urllib.request.urlretrieve(kafka_url, file_name)
    with tarfile.open(file_name, "r:gz") as tar:
        tar.extractall()
    os.remove(file_name)
    if file_name.replace('.tgz', '') != kafka_dir:
        os.rename(file_name.replace('.tgz', ''), kafka_dir)

# Function to update Kafka configuration with the network namespace IP and disable auto topic creation
def update_kafka_properties(kafka_dir, ns_ip, kafka_port):
    server_properties = os.path.join(kafka_dir, "config", "kraft", "server.properties")
    with open(server_properties, 'r') as file:
        properties = file.readlines()

    # Update the properties
    with open(server_properties, 'w') as file:
        auto_create_set = False
        for line in properties:
            # Update advertised.listeners with the namespace IP and port
            if line.startswith("advertised.listeners"):
                file.write(f"advertised.listeners=PLAINTEXT://{ns_ip}:{kafka_port}\n")
            # Set auto.create.topics.enable to false if it exists
            elif line.startswith("auto.create.topics.enable"):
                file.write("auto.create.topics.enable=false\n")
                auto_create_set = True
            # Set socket send, receive, and request max buffer bytes
            elif line.startswith("socket.send.buffer.bytes"):
                file.write(f"socket.send.buffer.bytes={args.socket_send_buffer_bytes}\n")
            elif line.startswith("socket.receive.buffer.bytes"):
                file.write(f"socket.receive.buffer.bytes={args.socket_receive_buffer_bytes}\n")
            elif line.startswith("socket.request.max.bytes"):
                file.write(f"socket.request.max.bytes={args.socket_request_max_bytes}\n")
            else:
                file.write(line)
        
        # If auto.create.topics.enable is not present, append it to the file
        if not auto_create_set:
            file.write("auto.create.topics.enable=false\n")
    
    print(f"Updated advertised.listeners to PLAINTEXT://{ns_ip}:{kafka_port}")
    print(f"Set socket.send.buffer.bytes to {args.socket_send_buffer_bytes}")
    print(f"Set socket.receive.buffer.bytes to {args.socket_receive_buffer_bytes}")
    print(f"Set socket.request.max.bytes to {args.socket_request_max_bytes}")
    print("Ensured auto.create.topics.enable is set to false.")

# Function to start Kafka in the network namespace
def start_kafka(kafka_dir):
    print("Starting Kafka broker in namespace...")
    kafka_server = os.path.join(kafka_dir, "bin", "kafka-server-start.sh")
    server_properties = os.path.join(kafka_dir, "config", "kraft", "server.properties")

    # Setting the environment variables for Kafka Heap and GC options
    env_vars = {
        "KAFKA_HEAP_OPTS": args.KAFKA_HEAP_OPTS,
        "KAFKA_JVM_PERFORMANCE_OPTS": f"-XX:MaxGCPauseMillis={args.MaxGCPauseMillis} -XX:G1ConcRefinementThreads={args.G1ConcRefinementThreads} -XX:ParallelGCThreads={args.G1ParallelGCThreads}"
    }

    # Construct the command to start Kafka with the environment variables
    command = (
        f"sudo ip netns exec {args.namespace} bash -c 'export KAFKA_HEAP_OPTS=\"{env_vars['KAFKA_HEAP_OPTS']}\" && "
        f"export KAFKA_JVM_PERFORMANCE_OPTS=\"{env_vars['KAFKA_JVM_PERFORMANCE_OPTS']}\" && "
        f"bash {kafka_server} {server_properties}'"
    )

    # Start the Kafka process
    return subprocess.Popen(command, shell=True)

# --------------------- LATENCY SETUP -----------------------------

if not namespace_exists(args.namespace):
    print(f"Creating network namespace {args.namespace}...")
    run_command(f"ip netns add {args.namespace}")
else:
    print(f"Namespace {args.namespace} already exists, skipping creation.")

print(f"Creating veth pair: {args.veth0} <-> {args.veth1}...")
run_command(f"ip link add {args.veth0} type veth peer name {args.veth1}")
run_command(f"ip link set {args.veth1} netns {args.namespace}")
run_command(f"ip addr add {args.host_ip} dev {args.veth0}")
run_command(f"ip link set {args.veth0} up")
run_command(f"ip netns exec {args.namespace} ip addr add {args.ns_ip} dev {args.veth1}")
run_command(f"ip netns exec {args.namespace} ip link set {args.veth1} up")
run_command(f"ip netns exec {args.namespace} ip link set lo up")

# Apply latency to veth0
print(f"Applying latency of {args.latency} to {args.veth0}...")
run_command(f"tc qdisc add dev {args.veth0} root netem delay {args.latency}")
run_command(f"sysctl -w net.ipv4.ip_forward=1")
run_command(f"ip route add {args.ns_ip.split('/')[0]}/32 dev {args.veth0}")

# --------------------- KAFKA SETUP -----------------------------

kafka_dir = "kafka_2.13-3.8.0"
kafka_url = "https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz"

# Download and extract Kafka if not present
if not os.path.isdir(kafka_dir):
    download_kafka(kafka_url, kafka_dir)

    # Update Kafka properties
    update_kafka_properties(kafka_dir, args.ns_ip.split('/')[0], args.kafka_port)

    # Initialize Kafka cluster
    initialize_kafka_cluster(kafka_dir, args.data_directory)

# Start Kafka inside the network namespace
kafka_process = start_kafka(kafka_dir)

# Wait for cleanup or manual interruption
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    cleanup()

