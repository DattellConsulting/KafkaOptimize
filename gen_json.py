import random
import string
import json
import os

def generate_unique_fields(count=500):
    fields = set()
    while len(fields) < count:
        field = ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10)))
        fields.add(field)
    return list(fields)

def assign_field_types(fields):
    return {field: random.choice(["string", "number", "boolean", "array"]) for field in fields}

def generate_random_value(value_type):
    if value_type == "string":
        return ''.join(random.choices(string.ascii_letters, k=random.randint(5, 20)))
    elif value_type == "number":
        return round(random.uniform(0, 1000), 2)
    elif value_type == "boolean":
        return random.choice([True, False])
    else:  # array
        return [random.randint(1, 100) for _ in range(3)]

def generate_json_log(fields, field_types):
    log_obj = {}
    current_length = 0
    while current_length < 400: 
        field = random.choice(fields)
        log_obj[field] = generate_random_value(field_types[field])
        log_str = json.dumps(log_obj, separators=(',', ':'))
        current_length = len(log_str)
    
    if current_length < 180 or current_length < 240:
        field = random.choice(fields)
        log_obj[field] = generate_random_value(field_types[field])
    
    return json.dumps(log_obj, separators=(',', ':'))

def generate_log_file(filename, target_size=10 * 1024 * 1024, field_count=500):
    fields = generate_unique_fields(field_count)
    field_types = assign_field_types(fields)

    current_size = 0
    with open(filename, 'w') as f:
        while current_size < target_size:
            log_message = generate_json_log(fields, field_types)

            f.write(log_message + '\n')
            current_size += len(log_message) + 1

    print(f"Log file generated: {filename}, size: {os.path.getsize(filename)} bytes")

# Configuration parameters
field_count = 500  # Default 500 unique fields
target_size = 10 * 1024 * 1024  # Default 10MB

# Generate log file
generate_log_file("large_flattened.json", target_size=target_size, field_count=field_count)

