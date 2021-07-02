# goal: retrieve Car_1 info
import boto3
import json

from awscrt.exceptions import AwsCrtError
from awsiot import mqtt_connection_builder
from awscrt import io, mqtt, auth
from uuid import uuid4
import threading
import sys

endpoint = "a2zyu38s23lujx-ats.iot.eu-central-1.amazonaws.com"
cert_filepath = "5bcd3cc02a-certificate.pem.crt"
pri_key_filepath = "5bcd3cc02a-private.pem.key"
ca_filepath = "root-CA.crt"
client_id = "test-" + str(uuid4())
programTopic = "test/topic"
programRegion = "eu-central-1"

# awsiot will make this event True when conditions are met
received_all_event = threading.Event()


# socket connection loss error handler callback
def on_connection_interrupted(connection, error, **kwargs):
    print(f"Connection interrupted. error: {error}")


# socket re-connection success callback
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print(f"Connection resumed. return_code: {return_code} session_present: {session_present}")

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))
    # topic - * , qos - 0, 1
    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit(f"Server rejected resubscribe to topic: {topic}")


def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print(f"Received message from topic '{topic}': {payload}")


# if __name__ == "main":
# initialize client
# iot = boto3.client('iot')
#
# # get current logging levels, format them as JSON, and write them to stdout
# response = iot.get_v2_logging_options()
# print(json.dumps(response, indent=4))

"""SETTING UP SOCKET CONNECTION HANDLER - START"""
# handles async computing - needed for dns resolver
event_loop_group = io.EventLoopGroup(1)
# resolves dns hosts - needed for client_boostrap
host_resolver = io.DefaultHostResolver(event_loop_group)
# creates socket connections
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
"""SETTING UP SOCKET CONNECTION HANDLER - END"""

credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)

mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
    websocket_proxy_options=None,
    credentials_provider=credentials_provider,
    region=programRegion,
    endpoint=endpoint,
    cert_filepath=cert_filepath,
    pri_key_filepath=pri_key_filepath,
    client_bootstrap=client_bootstrap,
    ca_filepath=ca_filepath,
    on_connection_interrupted=on_connection_interrupted,
    on_connection_resumed=on_connection_resumed,
    client_id=client_id,
    clean_session=False,
    keep_alive_secs=6)

# Connect
print(f"Connecting to {endpoint} with client ID '{client_id}'...")
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()
print("Connected!")

# Subscribe
# print(f"Subscribing to topic '{programTopic}'...")
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=programTopic,
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)

subscribe_result = subscribe_future.result()
print(f"Subscribed with {str(subscribe_result['qos'])}")
# Publish
mqtt_connection.publish(
    topic=programTopic,
    payload="""
            {
            "message": "Power On attempt",
            "deviceInstruction": "5057524f4e"
            }
            """,
    qos=mqtt.QoS.AT_LEAST_ONCE)

if not received_all_event.is_set():
    print("Waiting for all messages to be received...")

received_all_event.wait()

# Disconnect
print("Disconnecting...")
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")