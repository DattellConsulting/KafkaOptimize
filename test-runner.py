import subprocess
import sys
import os
import re
import time
import signal
import csv
from datetime import datetime
import yaml

# Load configuration from the YAML file
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

KAFKA_PARAMS_TO_TEST = config.get("KAFKA_PARAMS_TO_TEST", [])
client_parameters_to_test = config.get("client_parameters_to_test", [])
PARAMETERS = config.get("PARAMETERS", {})
client_parameters = config.get("client_parameters", {})
defaults = config.get("defaults", {})
client_defaults = config.get("client_defaults", {})

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
        writer.writerow([
            "Test Name", "Max Latency", "Avg Latency", "P99 Latency",
            "Messages Per Second", "Kafka Parameters", "Client Parameters"
        ])

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

def run_test(current_params, client_params, test_name):
    # Start Kafka setup process in the background
    kafka_setup_process = run_kafka_setup(current_params)

    # Wait until Kafka is ready to accept connections
    time.sleep(10)  # Adjust this based on Kafka startup time

    # Command line arguments for producer script
    producer_cmd = [
        'python3', 'latency.py',
        '--topic', 'test_topic',
        '--payload-file', 'large_flattened.json',
        '--max-latency', client_params['max_latency'],
        '--batch-size', client_params['batch_size'],
        '--acks', client_params['acks'],
        '--runtime', '10',
        '--warmup-time', '30',
        '--num-messages', client_params['num_messages'],
        '--compression-type', client_params['compression_type'],
        '--bootstrap-server', client_params['bootstrap_server'],
        '--consumer-count', client_params['consumer_count'],
        '--producer-count', client_params['producer_count'],
        '--fetch-wait-max-ms', client_params['fetch_wait_max_ms'],
        '--linger-ms', client_params['linger_ms'],
        '--max-in-flight-messages', client_params['max_in_flight_messages']
    ]

    # Run producer script and capture output
    print(f"Running producer with client parameters: {client_params}")
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
        print(line, end='')  # Optionally print producer output

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
    save_to_csv(test_name, current_params, client_params, max_latency, avg_latency, p99_latency, messages_per_second)

    return {
        'params': current_params.copy(),
        'client_params': client_params.copy(),
        'max_latency': max_latency,
        'avg_latency': avg_latency,
        'p99_latency': p99_latency,
        'messages_per_second': messages_per_second
    }

def extract_throughput(producer_output):
    # Parse the producer output to find the throughput value
    throughput_pattern = r"Total Throughput:\s+([0-9\.]+)\s+messages/sec"
    matches = re.findall(throughput_pattern, producer_output)
    if matches:
        return float(matches[-1])
    else:
        return 0.0

def extract_latency_metrics(producer_output):
    max_latency = extract_value(producer_output, r"Maximum Latency:\s+([0-9\.]+)\s+ms")
    avg_latency = extract_value(producer_output, r"Average Latency:\s+([0-9\.]+)\s+ms")
    p99_latency = extract_value(producer_output, r"99th Percentile Latency:\s+([0-9\.]+)\s+ms")
    return max_latency, avg_latency, p99_latency

def extract_value(producer_output, pattern):
    matches = re.findall(pattern, producer_output)
    if matches:
        return float(matches[-1])
    return 0.0

def save_to_csv(test_name, kafka_params, client_params, max_latency, avg_latency, p99_latency, messages_per_second):
    with open(CSV_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            test_name,
            max_latency,
            avg_latency,
            p99_latency,
            messages_per_second,
            kafka_params,
            client_params
        ])

def main():
    global defaults, client_defaults
    overall_best_result = {
        'messages_per_second': 0,
        'max_latency': 0,
        'avg_latency': 0,
        'p99_latency': 0,
        'params': defaults.copy(),
        'client_params': client_defaults.copy()
    }

    # PHASE 1: Test Kafka parameters only, keeping client parameters constant
    if KAFKA_PARAMS_TO_TEST:
        print("\n--- Testing Kafka Parameters ---")
        for kafka_param in KAFKA_PARAMS_TO_TEST:
            print(f"\nTesting Kafka parameter: {kafka_param}")
            best_kafka_result = None
            for value in PARAMETERS[kafka_param]:
                current_params = defaults.copy()
                current_params[kafka_param] = value
                test_name = f"{kafka_param}_{value}"

                # Use constant client parameters (client_defaults)
                result = run_test(current_params, client_defaults.copy(), test_name)

                if not best_kafka_result or result['messages_per_second'] > best_kafka_result['messages_per_second']:
                    best_kafka_result = result

            if best_kafka_result:
                defaults[kafka_param] = best_kafka_result['params'][kafka_param]
                if best_kafka_result['messages_per_second'] > overall_best_result['messages_per_second']:
                    overall_best_result = best_kafka_result

    # PHASE 2: Test client parameters only, keeping Kafka parameters constant
    if client_parameters_to_test:
        print("\n--- Testing Client Parameters ---")
        for client_param in client_parameters_to_test:
            print(f"\nTesting client parameter: {client_param}")
            best_client_result = None
            for value in client_parameters[client_param]:
                current_client_params = client_defaults.copy()
                current_client_params[client_param] = value
                test_name = f"{client_param}_{value}"

                result = run_test(defaults.copy(), current_client_params, test_name)

                if not best_client_result or result['messages_per_second'] > best_client_result['messages_per_second']:
                    best_client_result = result

            if best_client_result:
                client_defaults[client_param] = best_client_result['client_params'][client_param]
                if best_client_result['messages_per_second'] > overall_best_result['messages_per_second']:
                    overall_best_result = best_client_result

    # Output the overall best result
    print("\nOverall Best Result:")
    print(f"Messages/sec = {overall_best_result['messages_per_second']}")
    print(f"Max Latency = {overall_best_result['max_latency']}")
    print(f"Avg Latency = {overall_best_result['avg_latency']}")
    print(f"P99 Latency = {overall_best_result['p99_latency']}")
    print(f"Kafka Parameters: {overall_best_result['params']}")
    print(f"Client Parameters: {overall_best_result['client_params']}")

if __name__ == '__main__':
    main()

