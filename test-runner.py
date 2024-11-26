import subprocess
import sys
import os
import re
import time
import signal
import csv
from datetime import datetime


# Overall Best Result:
# Messages/sec = 287771.62
# Max Latency = 25.12
# Avg Latency = 7.91
# P99 Latency = 11.57
# Kafka Parameters: {'KAFKA_HEAP_OPTS': '-Xms8G -Xmx8G', 'MaxGCPauseMillis': '5', 'G1ConcRefinementThreads': '16', 'G1ParallelGCThreads': '16', 'KAFKA_NUM_NETWORK_THREADS': '3', 'KAFKA_NUM_IO_THREADS': '8', 'KAFKA_NUM_PARTITIONS': '1', 'socket_send_buffer_bytes': '524288', 'socket_receive_buffer_bytes': '524288', 'socket_request_max_bytes': '104857600', 'latency': '2ms'}
# Latency Parameters: {'batch_size': '131072', 'compression_type': 'snappy', 'consumer_count': '5', 'producer_count': '5', 'fetch_wait_max_ms': '5', 'partitions': '10', 'bootstrap_server': '10.10.1.2:9092', 'linger_ms': '5', 'max_in_flight_messages': '5', 'max_latency': '50', 'acks': '-1', 'num_messages': '10000'}
# Specify which parameters to test (both Kafka and latency.py)

#KAFKA_PARAMS_TO_TEST = ['KAFKA_HEAP_OPTS', 'MaxGCPauseMillis', 'KAFKA_NUM_NETWORK_THREADS', 'KAFKA_NUM_IO_THREADS', 'socket_send_buffer_bytes', 'socket_receive_buffer_bytes']
#LATENCY_PARAMS_TO_TEST = ['consumer_count', 'partitions', 'producer_count', 'fetch_wait_max_ms']

KAFKA_PARAMS_TO_TEST = []
LATENCY_PARAMS_TO_TEST = ['batch_size']

# Define possible values for each Kafka parameter
PARAMETERS = {
    'KAFKA_HEAP_OPTS': ["-Xms2G -Xmx2G", "-Xms4G -Xmx4G", "-Xms8G -Xmx8G", "-Xms12G -Xmx12G"],
    'MaxGCPauseMillis': ["1", "5", "10", "20", "40"],
    'G1ConcRefinementThreads': ["4", "8", "16", "32"],
    'G1ParallelGCThreads': ["4", "8", "16", "32"],
    'KAFKA_NUM_NETWORK_THREADS': ["4", "8", "16", "32"],
    'KAFKA_NUM_IO_THREADS': ["8", "16", "32"],
    'socket_send_buffer_bytes': ["102400", "524288", "1048576"],
    'socket_receive_buffer_bytes': ["102400", "524288", "1048576"]
}

# Define possible values for latency.py parameters
LATENCY_PARAMETERS = {
    # 'batch_size': ["128", "256", "512", "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072", "262144", "524288"],
    'batch_size': ["512", "1024", "2048", "4096", "8192", "16384"],
    'compression_type': ["none", "snappy", "zlib", "gzip"],
    'consumer_count': ["2", "5", "10"],
    'partitions': ["1", "5", "10", "20", "40"],
    'producer_count': ["5", "10", "20", "40"],
    'fetch_wait_max_ms': ["1", "2", "5", "10", "20", "40"],
    'linger_ms': ["0", "1", "2", "5", "10", "20"],
    'max_in_flight_messages': ["1", "2", "5"]
}

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

# Ensure Kafka is stopped and cleaned up before running the setup
def clean_up_environment():
    # Stop Kafka if running
    try:
        kafka_pids = subprocess.check_output(["pgrep", "-f", "server.properties"]).decode().strip()
        if kafka_pids:
            # Split the PIDs and terminate each one
            for kafka_pid in kafka_pids.split('\n'):
                print(f"Stopping Kafka (PID: {kafka_pid})...")
                os.kill(int(kafka_pid), signal.SIGTERM)
            time.sleep(5)  # Give some time for Kafka to stop
    except subprocess.CalledProcessError:
        print("Kafka is not running.")

    # Delete veth0 interface if it exists
    try:
        subprocess.run(["ip", "link", "delete", "veth0"], check=True)
        print("Successfully deleted veth0 interface.")
    except subprocess.CalledProcessError:
        print("veth0 interface does not exist or failed to delete.")

# Call cleanup before running Kafka setup
clean_up_environment()

# CSV output file
test_time = datetime.now().strftime('%Y-%m-%d-%H-%M')
CSV_FILE = "test_results/test_results.csv" + str(test_time)

# Write CSV header if the file doesn't exist
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Test Name", "Max Latency", "Avg Latency", "P99 Latency", "Messages Per Second", "Kafka Parameters", "Latency Parameters"])

def run_kafka_setup(current_params):
    # Command line arguments for Kafka setup script
    kafka_setup_cmd = [
        'nohup', 'python3', 'setup_kafka.py',
        '--data-directory', '/mnt/data/kafkatest',
        '--KAFKA_HEAP_OPTS', current_params['KAFKA_HEAP_OPTS'],
        '--MaxGCPauseMillis', current_params['MaxGCPauseMillis'],
        '--G1ConcRefinementThreads', current_params['G1ConcRefinementThreads'],
        '--G1ParallelGCThreads', current_params['G1ParallelGCThreads'],
        '--KAFKA_NUM_NETWORK_THREADS', current_params['KAFKA_NUM_NETWORK_THREADS'],
        '--KAFKA_NUM_IO_THREADS', current_params['KAFKA_NUM_IO_THREADS'],
        '--socket-send-buffer-bytes', current_params['socket_send_buffer_bytes'],
        '--socket-receive-buffer-bytes', current_params['socket_receive_buffer_bytes'],
        '--socket-request-max-bytes', current_params['socket_request_max_bytes'],
        '--KAFKA_NUM_PARTITIONS', '1',
        '--latency', current_params['latency']
    ]

    # Start Kafka using the setup script in the background using nohup
    print(f"Starting Kafka with parameters: {current_params}")
    with open("kafka_setup.log", "w") as outfile:
        kafka_setup_process = subprocess.Popen(
            kafka_setup_cmd,
            stdout=outfile,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid  # Start the process in a new session
        )
        time.sleep(2)
    return kafka_setup_process

def run_test(current_params, latency_params, test_name):
    # Start Kafka setup process in the background
    kafka_setup_process = run_kafka_setup(current_params)

    # Wait until Kafka is ready to accept connections
    time.sleep(10)  # Adjust this based on Kafka startup time

    # Command line arguments for producer script
    producer_cmd = [
        'python3', 'latency.py',
        '--topic', 'test_topic',
        '--payload-file', 'large_flattened.json',
        '--max-latency', latency_params['max_latency'],
        '--batch-size', latency_params['batch_size'],
        '--acks', latency_params['acks'],
        '--runtime', '10',
        '--warmup-time', '30',
        '--num-messages', latency_params['num_messages'],
        '--compression-type', latency_params['compression_type'],
        '--bootstrap-server', latency_params['bootstrap_server'],
        '--consumer-count', latency_params['consumer_count'],
        '--producer-count', latency_params['producer_count'],
        '--fetch-wait-max-ms', latency_params['fetch_wait_max_ms'],
        '--linger-ms', latency_params['linger_ms'],
        '--max-in-flight-messages', latency_params['max_in_flight_messages']
    ]

    # Run producer script and capture output
    print(f"Running producer with params: {latency_params}")
    producer_process = subprocess.Popen(
        producer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Read producer output in real-time
    producer_output = ''
    while True:
        line = producer_process.stdout.readline()
        if not line:
            if producer_process.poll() is not None:
                break
            else:
                continue
        producer_output += line
        print(line, end='')  # Optional: print producer output

    # Check if producer exited successfully
    if producer_process.returncode != 0:
        print("Producer exited with errors.")
        print(producer_output)
    else:
        print("Producer completed successfully.")

    # Terminate the Kafka process
    print("Stopping Kafka...")
    os.killpg(os.getpgid(kafka_setup_process.pid), signal.SIGTERM)
    kafka_setup_process.wait()

    # Extract results from the output
    max_latency, avg_latency, p99_latency = extract_latency_metrics(producer_output)
    messages_per_second = extract_throughput(producer_output)

    # Save the test output to CSV
    save_to_csv(test_name, current_params, latency_params, max_latency, avg_latency, p99_latency, messages_per_second)

    return {
        'params': current_params.copy(),
        'latency_params': latency_params.copy(),
        'max_latency': max_latency,
        'avg_latency': avg_latency,
        'p99_latency': p99_latency,
        'messages_per_second': messages_per_second
    }

def extract_throughput(producer_output):
    # Parse the producer output to find the throughput value
    # Matches: "Total Throughput: 17074.00 messages/sec"
    throughput_pattern = r"Total Throughput:\s+([0-9\.]+)\s+messages/sec"
    matches = re.findall(throughput_pattern, producer_output)
    if matches:
        # Take the last throughput value reported
        return float(matches[-1])
    else:
        return 0.0

def extract_latency_metrics(producer_output):
    # Matches for latency metrics
    max_latency = extract_value(producer_output, r"Maximum Latency:\s+([0-9\.]+)\s+ms")
    avg_latency = extract_value(producer_output, r"Average Latency:\s+([0-9\.]+)\s+ms")
    p99_latency = extract_value(producer_output, r"99th Percentile Latency:\s+([0-9\.]+)\s+ms")
    return max_latency, avg_latency, p99_latency

def extract_value(producer_output, pattern):
    matches = re.findall(pattern, producer_output)
    if matches:
        return float(matches[-1])
    return 0.0

def save_to_csv(test_name, kafka_params, latency_params, max_latency, avg_latency, p99_latency, messages_per_second):
    with open(CSV_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            test_name,
            max_latency,
            avg_latency,
            p99_latency,
            messages_per_second,
            kafka_params,
            latency_params
        ])

def main():
    global defaults, latency_defaults
    overall_best_result = {
        'messages_per_second': 0,
        'max_latency': 0,
        'avg_latency': 0,
        'p99_latency': 0,
        'params': defaults.copy(),
        'latency_params': latency_defaults.copy()
    }

    # PHASE 1: Test Kafka parameters only, keeping latency parameters constant
    if KAFKA_PARAMS_TO_TEST:
        print("\n--- Testing Kafka Parameters ---")
        for kafka_param in KAFKA_PARAMS_TO_TEST:
            print(f"\nTesting Kafka parameter: {kafka_param}")
            best_kafka_result = None
            for value in PARAMETERS[kafka_param]:
                current_params = defaults.copy()
                current_params[kafka_param] = value
                test_name = f"{kafka_param}_{value}"

                # Keep latency parameters constant (use latency_defaults)
                result = run_test(current_params, latency_defaults.copy(), test_name)

                # Track the best result for Kafka parameter based on messages per second
                if not best_kafka_result or result['messages_per_second'] > best_kafka_result['messages_per_second']:
                    best_kafka_result = result

            # Update defaults with the best Kafka parameter value
            if best_kafka_result:
                defaults[kafka_param] = best_kafka_result['params'][kafka_param]

                # Update the overall best result if this Kafka parameter performed better
                if best_kafka_result['messages_per_second'] > overall_best_result['messages_per_second']:
                    overall_best_result = best_kafka_result

    # PHASE 2: Test latency parameters only, keeping Kafka parameters constant
    if LATENCY_PARAMS_TO_TEST:
        print("\n--- Testing Latency Parameters ---")
        for lat_param in LATENCY_PARAMS_TO_TEST:
            print(f"\nTesting latency.py parameter: {lat_param}")
            best_latency_result = None
            for lat_value in LATENCY_PARAMETERS[lat_param]:
                latency_params = latency_defaults.copy()
                latency_params[lat_param] = lat_value
                test_name = f"{lat_param}_{lat_value}"

                # Keep Kafka parameters constant (use defaults)
                result = run_test(defaults.copy(), latency_params, test_name)

                # Track the best result for latency parameter based on messages per second
                if not best_latency_result or result['messages_per_second'] > best_latency_result['messages_per_second']:
                    best_latency_result = result

            # Update latency_defaults with the best latency parameter value
            if best_latency_result:
                latency_defaults[lat_param] = best_latency_result['latency_params'][lat_param]

                # Update the overall best result if this latency parameter performed better
                if best_latency_result['messages_per_second'] > overall_best_result['messages_per_second']:
                    overall_best_result = best_latency_result

    # Output the overall best result
    print("\nOverall Best Result:")
    print(f"Messages/sec = {overall_best_result['messages_per_second']}")
    print(f"Max Latency = {overall_best_result['max_latency']}")
    print(f"Avg Latency = {overall_best_result['avg_latency']}")
    print(f"P99 Latency = {overall_best_result['p99_latency']}")
    print(f"Kafka Parameters: {overall_best_result['params']}")
    print(f"Latency Parameters: {overall_best_result['latency_params']}")


if __name__ == '__main__':
    main()

