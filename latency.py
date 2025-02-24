import time
import json
import argparse
import multiprocessing
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from datetime import datetime, timezone

# Function to delete and recreate the Kafka topic
def recreate_topic(bootstrap_server, topic_name, partitions, replication_factor):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_server})

    # Delete topic if it exists
    fs = admin_client.delete_topics([topic_name], operation_timeout=30)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Deleted topic: {topic_name}")
        except KafkaException as e:
            print(f"Failed to delete topic {topic_name}: {e}")

    # Verify topic deletion by checking metadata
    timeout = 30  # max time to wait for deletion in seconds
    interval = 2   # polling interval in seconds
    elapsed_time = 0

    while elapsed_time < timeout:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"Topic {topic_name} successfully deleted.")
            break
        else:
            print(f"Waiting for topic {topic_name} deletion to propagate...")
            time.sleep(interval)
            elapsed_time += interval
    else:
        print(f"Timed out waiting for topic {topic_name} to be deleted.")
        return

    time.sleep(10)

    # Recreate topic
    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Created topic: {topic_name}")
        except KafkaException as e:
            print(f"Failed to create topic {topic_name}: {e}")

    time.sleep(1)

# Kafka Producer
def produce_messages(producer, topic_name, num_messages, warmup_time, runtime, messages):
    start_time = time.time()
    messages_sent = 0
    message_index = 0
    total_messages_in_file = len(messages)

    # Produce messages during both warmup and runtime
    while time.time() - start_time < warmup_time + runtime:
        current_time = time.time()

        # Attempt to publish all messages every second
        for _ in range(num_messages):
            message = messages[message_index % total_messages_in_file]
            timestamp = datetime.now(timezone.utc).timestamp()
            message_index += 1

            try:
                producer.produce(
                    topic_name,
                    key=str(message_index),
                    value=json.dumps(message),
                    headers={'timestamp': str(timestamp)}
                )
            except KafkaException as e:
                print(f"Failed to send message {message_index}: {e}")

        producer.flush()
        print(f"Attempted to produce {num_messages} messages in this second.")

        messages_sent += num_messages

        # Sleep until the next second
        elapsed_time = time.time() - current_time
        if elapsed_time < 1:
            time.sleep(1 - elapsed_time)

    # Send final "marker" message to signal producer completion
    producer.produce(
        topic_name,
        key="final",
        value=json.dumps({"message": "end"}),
        headers={'timestamp': str(datetime.now(timezone.utc).timestamp())}
    )
    producer.flush()

    end_time = time.time()
    print(f"Producer finished. Sent {messages_sent} messages in {end_time - start_time} seconds.")


# Kafka Consumer
def consume_messages(consumer, topic_name, warmup_time, runtime, result_queue):
    latencies = []
    message_count = 0
    throughput_count = 0

    start_latency_measurement = time.time() + warmup_time
    end_time = start_latency_measurement + runtime

    while time.time() < end_time:
        msg = consumer.poll(timeout=0.1)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        consumed_time = datetime.now(timezone.utc).timestamp()
        headers = dict(msg.headers())

        if 'timestamp' in headers:
            produced_time = float(headers['timestamp'])
            latency = (consumed_time - produced_time) * 1000  # latency in ms

            if time.time() >= start_latency_measurement:
                latencies.append(latency)
                message_count += 1
                throughput_count += 1

        # Detect the final "marker" message and break the loop
        if msg.key().decode('utf-8') == "final":
            print("Received producer completion signal. Ending consumption.")
            break

    # Calculate throughput and latency stats
    if throughput_count > 0:
        total_time = time.time() - start_latency_measurement
        throughput = throughput_count / total_time

        average_latency = sum(latencies) / len(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0
        latency_99th = sorted(latencies)[int(0.99 * len(latencies))] if latencies else 0

        # Send the results back to the main process via the result queue
        result_queue.put({
            'throughput': throughput,
            'average_latency': average_latency,
            'latency_99th': latency_99th,
            'max_latency': max_latency,
            'message_count': message_count
        })

    print(f"Consumer finished. Consumed {message_count} messages.")


def start_producer(bootstrap_server, topic_name, num_messages, warmup_time, runtime, batch_size, acks, max_in_flight_requests_per_connection, compression_type, linger_ms, queue_buffering_max_messages, messages):
    producer = Producer({
        'bootstrap.servers': bootstrap_server,
        'batch.size': batch_size,
        'acks': acks,
        'max.in.flight.requests.per.connection': max_in_flight_requests_per_connection,
        'compression.type': compression_type,  # Set compression type here
        'linger.ms': linger_ms,  # Added linger.ms parameter
        'queue.buffering.max.messages': queue_buffering_max_messages  # Added queue buffering max messages
    })
    print("Starting producer. Warmup and producing messages.")
    produce_messages(producer, topic_name, num_messages, warmup_time, runtime, messages)


def start_consumer(bootstrap_server, topic_name, fetch_wait_max_ms, warmup_time, runtime, result_queue):
    consumer = Consumer({
        'bootstrap.servers': bootstrap_server,
        'group.id': 'latency-test-group',
        'auto.offset.reset': 'earliest',
        'fetch.wait.max.ms': fetch_wait_max_ms
    })

    consumer.subscribe([topic_name])

    print("Starting consumer. Warmup and consuming messages.")
    consume_messages(consumer, topic_name, warmup_time, runtime, result_queue)
    consumer.close()


def run_test(args, messages_per_second, messages):
    # Recreate the topic before starting producer and consumer
    recreate_topic(args.bootstrap_server, args.topic_name, args.partitions, args.replication_factor)

    # Queue to collect results from consumer processes
    result_queue = multiprocessing.Queue()

    # Create separate producer processes
    producer_processes = []
    for _ in range(args.producer_count):
        producer_process = multiprocessing.Process(target=start_producer, args=(
            args.bootstrap_server, args.topic_name, messages_per_second // args.producer_count, args.warmup_time, args.runtime, args.batch_size, args.acks, args.max_in_flight_messages, args.compression_type, args.linger_ms, args.queue_buffering_max_messages, messages))
        producer_processes.append(producer_process)
        producer_process.start()

    # Create separate consumer processes
    consumer_processes = []
    for _ in range(args.consumer_count):
        consumer_process = multiprocessing.Process(target=start_consumer, args=(
            args.bootstrap_server, args.topic_name, args.fetch_wait_max_ms, args.warmup_time, args.runtime, result_queue))
        consumer_processes.append(consumer_process)
        consumer_process.start()

    # Wait for all producer processes to finish
    for producer_process in producer_processes:
        producer_process.join()

    # Wait for all consumer processes to finish
    for consumer_process in consumer_processes:
        consumer_process.join()

    # Aggregate results from all consumers
    total_throughput = 0
    total_message_count = 0
    total_average_latency = 0
    total_99th_latency = 0
    total_max_latency = 0
    consumers_reported = 0

    while not result_queue.empty():
        result = result_queue.get()
        total_throughput += result['throughput']
        total_message_count += result['message_count']
        total_average_latency += result['average_latency']
        total_99th_latency += result['latency_99th']
        total_max_latency = max(total_max_latency, result['max_latency'])
        consumers_reported += 1

    if consumers_reported > 0:
        total_average_latency /= consumers_reported
        total_99th_latency /= consumers_reported

    # Print final aggregated results
    print(f"Total Throughput: {total_throughput:.2f} messages/sec")
    print(f"Average Latency: {total_average_latency:.2f} ms")
    print(f"99th Percentile Latency: {total_99th_latency:.2f} ms")
    print(f"Maximum Latency: {total_max_latency:.2f} ms")

    return total_max_latency, total_message_count, total_throughput, total_average_latency, total_99th_latency


# Main function to run producers and consumers in parallel using multiprocessing
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Latency Test Script")
    parser.add_argument('--bootstrap-server', type=str, default="10.10.1.2:9092", help='Kafka broker address')
    parser.add_argument('--topic-name', type=str, default="test_topic", help='Kafka topic name')
    parser.add_argument('--num-messages', type=int, default=1000, help='Total number of messages per second')
    parser.add_argument('--warmup-time', type=int, default=10, help='Warmup time in seconds')
    parser.add_argument('--runtime', type=int, default=10, help='Runtime in seconds')
    parser.add_argument('--partitions', type=int, default=10, help='Number of partitions')
    parser.add_argument('--replication-factor', type=int, default=1, help='Replication factor')
    parser.add_argument('--batch-size', type=int, default=65536, help='Kafka producer batch size')
    parser.add_argument('--acks', type=str, default='-1', help='Kafka producer acks')
    parser.add_argument('--producer-count', type=int, default=5, help='Number of producer processes')
    parser.add_argument('--consumer-count', type=int, default=5, help='Number of consumer processes')
    parser.add_argument('--fetch-wait-max-ms', type=int, default=5, help='Max wait time for consumer fetch in ms')
    parser.add_argument('--max-latency', type=int, default=50, help='Maximum allowed latency in ms')
    parser.add_argument('--max-in-flight-messages', type=int, default=5, help='Maximum number of in-flight messages per connection')
    parser.add_argument('--compression-type', type=str, default='none', help='Compression type for the producer')
    parser.add_argument('--linger-ms', type=int, default=5, help='Producer linger.ms setting') 
    parser.add_argument('--queue-buffering-max-messages', type=int, default=10000000, help='Maximum number of messages allowed on the producer queue')  
    parser.add_argument('--payload-file', type=str, required=True, help='Path to the JSON file containing message payloads')

    args = parser.parse_args()

    # Load messages from JSON file
    with open(args.payload_file, 'r') as f:
        messages = [json.loads(line) for line in f]

    # Start with the initial message rate
    messages_per_second = args.num_messages
    latency_exceeded = False

    # Continue increasing message rate until max latency is exceeded, then decrease until it's below max latency
    while True:
        total_max_latency, total_message_count, total_throughput, total_average_latency, total_99th_latency = run_test(args, messages_per_second, messages)

        # Check the percentage of processed messages 
        message_processed_pct = round(total_message_count / (messages_per_second * args.runtime) * 100, 4)
        print(f"{message_processed_pct}% of messages processed.")

        if total_max_latency > args.max_latency:
            print(f"Max latency {total_max_latency:.2f} ms exceeded the threshold.")
            latency_exceeded = True

        if latency_exceeded:
            if total_max_latency <= args.max_latency:
                print(f"Latency is now below the maximum threshold after reduction. Ending test.")
                print(f"Total Throughput: {total_throughput:.2f} messages/sec")
                print(f"Average Latency: {total_average_latency:.2f} ms")
                print(f"99th Percentile Latency: {total_99th_latency:.2f} ms")
                print(f"Maximum Latency: {total_max_latency:.2f} ms")
                break
            else:
                print(f"Reducing message rate by 20% to bring latency back under the threshold.")
                messages_per_second = int(messages_per_second * 0.9)
        else:
            print(f"Doubling message rate since latency {total_max_latency:.2f} ms is below the threshold.")
            messages_per_second *= 2

        # Sleep before the next iteration
        print(f"New message rate: {messages_per_second} messages/sec.")
        time.sleep(2)

