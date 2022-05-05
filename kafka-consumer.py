from confluent_kafka import Consumer, KafkaError


def lambda_handler(event, context):
    bootstrap_server = event['bootstrap_server']
    groupid = event['groupid']
    offset = event['offset']
    topic = event['topic']
    c = Consumer({
        'bootstrap.servers': bootstrap_server,
        'group.id': groupid,
        'default.topic.config': {
            'auto.offset.reset': offset
        }
    })

    c.subscribe([topic])
    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

        c.close()
        return {
            "Status": "Success",
            "Info": "Message Received - {}".format(msg.value().decode('utf-8'))
        }
    except:
        return {
            "Status": "TimedOut",
            "Info":"Running more than Scheduled Timedout. "
        }