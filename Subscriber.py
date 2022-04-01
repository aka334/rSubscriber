#!/usr/bin/env python
import pika
import sys
import json

# Set the connection parameters to connect to rabbit-server1 on port 5672
# on the / virtual host using the username "guest" and password "guest"

username = ''
password = ''
hostname = ''
virtualhost = ''


credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(hostname,
                                           5672,
                                           virtualhost,
                                           credentials)

connection = pika.BlockingConnection(parameters)

channel = connection.channel()

exchange_name = 'patient_data'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

binding_keys = "#"

if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(
        exchange=exchange_name, queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

    testing_data = json.loads(body)
    for test in testing_data:
        print("*Python Class*")
        print("\ttesting_id: " + str(test['testing_id']))
        print("\tpatient_name: " + str(test['patient_name']))
        print("\tpatient_mrn: " + str(test['patient_mrn']))
        print("\tpatient_zipcode: " + str(test['patient_zipcode']))
        print("\tpatient_status: " + str(test['patient_status']))
        print("\tcontact_list: " + str(test['contact_list']))
        print("\tevent_list: " + str(test['event_list']))



channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

