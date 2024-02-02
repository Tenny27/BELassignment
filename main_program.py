import json
import threading
from datetime import datetime
import queue
import pika

status1 = queue.Queue()
status2 = queue.Queue()
final_queue = queue.Queue()

user_id = int(input("Enter User id for Simulation 1: "))
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

def main1(status_queue):
    s = simulation_program1(user_id, timestamp)
    status_queue.put(s)
    print(status_queue)

def simulation_program1(user_id, timestamp):
    status = int(input(f"Enter status for user {user_id} at {timestamp}: "))
    return status if status in [1, 0, -1] else None

simulation1_thread = threading.Thread(target=main1, args=(status1,))
simulation1_thread.start()

def main2(status_queue):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    s = simulation_program2(user_id, timestamp)
    status_queue.put(s)
    print(status_queue)

def simulation_program2(user_id, timestamp):
    status = int(input(f"Enter status for user {user_id} at {timestamp}: "))
    return status if status in [1, 0, -1] else None

simulation2_thread = threading.Thread(target=main2, args=(status2,))
simulation2_thread.start()

simulation1_thread.join()
simulation2_thread.join()

status1_value = status1.get()
status2_value = status2.get()

def print_final_status(final_status):
    print("Final Status:", final_status)

if status1_value in [0, 1] and status2_value in [0, 1]:
    final_status = min(status1_value, status2_value)
    print_final_status(final_status)
elif status1_value == -1 and status2_value is not None:
    final_status = status2_value
    print_final_status(final_status)
elif status1_value is not None and status2_value == -1:
    final_status = status1_value
    print_final_status(final_status)
else:
    final_status = -1
    print_final_status(final_status)

final_queue.put([user_id, timestamp, final_status])

def message_producer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    message_data = {'user_id': user_id, 'timestamp': timestamp, 'final_status': final_status}
    message_body = json.dumps(message_data)
    channel.queue_declare(queue='Main_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='Main_queue', body=message_body)
    print(" [x] Sent Data")
    connection.close()
message_producer()
