import pika
import json
import sys
import psycopg2

DB_CONNECTION_PARAMS = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '1967',
    'dbname': 'postgres'
}

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='Main_queue', durable=True)

    def callback(ch, method, properties, body):
        message_data = json.loads(body)

        user_id = message_data.get('user_id')
        timestamp = message_data.get('timestamp')
        final_status = message_data.get('final_status')

        print(f"Received message: User {user_id}, Timestamp {timestamp}, Final Status {final_status}")
        update_in_postgres(user_id, timestamp, final_status)

    channel.basic_consume(queue='Main_queue', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def get_final_status(user_id, timestamp):
    try:
        connection = psycopg2.connect(**DB_CONNECTION_PARAMS)
        cursor = connection.cursor()
        sql_command = """
            SELECT final_status
            FROM main_table
            WHERE user_id = %s AND timestamp = %s;
        """

        cursor.execute(sql_command, (user_id, timestamp))
        result = cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            print(f"No data found for User {user_id}, Timestamp {timestamp}")
            return None

    except psycopg2.DatabaseError as error:
        print(f"Error retrieving data from the database: {error}")
        return None

    finally:
        if connection:
            cursor.close()
            connection.close()

def update_in_postgres(user_id, timestamp, final_status):
    try:
        connection = psycopg2.connect(**DB_CONNECTION_PARAMS)
        cursor = connection.cursor()

        existing_status = get_final_status(user_id, timestamp)

        if existing_status in [0,1] and final_status in [0,1]:
            final_status = min(existing_status, final_status)
        elif existing_status==-1 and final_status in [0,1]:
            final_status=final_status
        elif existing_status in [0,1] and  final_status==-1:
            final_status=existing_status
        sql_command = """
            INSERT INTO main_table (user_id, timestamp, final_status)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, timestamp)
            DO UPDATE SET final_status = %s;
        """

        cursor.execute(sql_command, (user_id, timestamp, final_status, final_status))

        # Commit the changes
        connection.commit()
        print(f"Database updated successfully: User {user_id}, Timestamp {timestamp}, Final Status {final_status}")

    except psycopg2.DatabaseError as error:
        print(f"Error updating database: {error}")

    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(0)
