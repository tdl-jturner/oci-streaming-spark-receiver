#!/usr/local/bin/python
import oci
import sys
import time
from base64 import b64encode, b64decode
import argparse

"""
This is as a python script that utilizes the OCI CLI to interact with streams. You must first set up the OCI CLI by following the instructions at https://docs.cloud.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm

I.E. Produce data with -- while true; do ./stream.py --produce "DateLoop:`date`";sleep 1;done
"""

def get_or_create_stream(client, compartment_id, stream_name, partition, sac_composite):
    """ Get or create the given stream"""

    list_streams = client.list_streams(compartment_id, name=stream_name,
                                       lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("An active stream {} has been found".format(stream_name))
        sid = list_streams.data[0].id
        return sac_composite.client.get_stream(sid)

    else:
        print(" No Active stream  {} has been found; Creating it now. ".format(stream_name))
        print(" Creating stream {} with {} partitions.".format(stream_name, partition))

        # Create stream_details object that need to be passed while creating stream.
        stream_details = oci.streaming.models.CreateStreamDetails(name=stream_name, partitions=partition, compartment_id=compartment, retention_in_hours=24)

        # Since stream creation is asynchronous; we need to wait for the stream to become active.
        response = sac_composite.create_stream_and_wait_for_state( stream_details, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE])
        return response

def get_cursor_by_group(sc, sid, group_name, instance_name):
    """ Create a new group cursor """
    print(" Creating a cursor for group {}, instance {}".format(group_name, instance_name))
    cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name, instance_name=instance_name,
                                                                   type=oci.streaming.models.
                                                                   CreateGroupCursorDetails.TYPE_TRIM_HORIZON,
                                                                   commit_on_get=True)
    response = sc.create_group_cursor(sid, cursor_details)
    return response.data.value

def consume(client, stream_id):
    """ Consume a stream """
    cursor = get_cursor_by_group(client, stream_id, args.group, args.instance)
    while True:
        try:
            get_response = client.get_messages(stream_id, cursor, limit=args.limit)
        except oci.exceptions.ServiceError:
            cursor = get_cursor_by_group(client, stream_id, args.group, args.instance)
            continue
        if get_response.data:
            for message in get_response.data:
                if message.key != None:
                    key = b64decode(message.key.encode()).decode()
                else:
                    key=None
                print("{}: {}".format(key, b64decode(message.value.encode()).decode()))
            cursor = get_response.headers["opc-next-cursor"]

        time.sleep(1)

def produce(client,  stream_id, message):
    """ Produce a message to a stream """
    if ':' in message:
        key=message[0:message.index(':')]
        value=message[message.index(':')+1:]
        encoded_key = b64encode(key.encode()).decode()
    else:
        encoded_key = None
        value=message
    encoded_value = b64encode(value.encode()).decode()

    message_list = []
    message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))

    print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
    messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
    put_message_result = client.put_messages(stream_id, messages)

    # The put_message_result can contain some useful metadata for handling failures
    for entry in put_message_result.data.entries:
        if entry.error:
            print("Error ({}) : {}".format(entry.error, entry.error_message))
        else:
            print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))

def delete_stream(client, stream_id):
    """Delete a stream"""
    print(" Deleting Stream {}".format(stream_id))
    print("  Stream deletion is an asynchronous operation, give it some time to complete.")
    client.delete_stream_and_wait_for_state(stream_id, wait_for_states=[oci.streaming.models.StreamSummary.LIFECYCLE_STATE_DELETED])


parser = argparse.ArgumentParser(description='Interact with OCI Stream')
parser.add_argument('--stream', default="StreamTest", help='name of stream')
parser.add_argument('--partitions', type=int, default=1, help='number of partitions')
parser.add_argument('--compartment', help='UUID of compartment')
parser.add_argument('--consume', help='Consume the stream and print messages', action='store_true')
parser.add_argument('--group', default="DefaultGroup", help='Name of Consumer Group')
parser.add_argument('--instance', default="DefaultInstance", help='Name of Consumer Instance')
parser.add_argument('--limit', type=int, default=10, help='Limit number of consumed messages')
parser.add_argument('--produce', metavar='MESSAGE', help='Produce a message')
parser.add_argument('--delete', action='store_true', help='Delete stream')
args = parser.parse_args()

print args

if args.consume == False and args.produce == None and args.delete==False:
    raise RuntimeError("You must specify --produce, --consume, or --delete")

config = oci.config.from_file()
stream_admin_client = oci.streaming.StreamAdminClient(config)
stream_admin_client_composite = oci.streaming.StreamAdminClientCompositeOperations(stream_admin_client)

if args.compartment:
    compartment = args.compartment
else:
    compartment = config['compartment']

stream = get_or_create_stream(stream_admin_client, compartment, args.stream, args.partitions, stream_admin_client_composite).data

print(" Opened Stream {} with id : {}".format(stream.name, stream.id))

stream_client = oci.streaming.StreamClient(config, service_endpoint=stream.messages_endpoint)
s_id = stream.id

if args.consume:
    consume(stream_client,s_id)

if args.produce:
    produce(stream_client,s_id,args.produce)

if args.delete:
    delete_stream(stream_admin_client_composite, s_id)
