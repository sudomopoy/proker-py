from proker.factory import MessageBrokerFactory

factory = MessageBrokerFactory()

consumer = factory.get_consumer("rabbitmq",{})

consumer.connect()
consumer.declare_queue('test')
consumer.bind_queue('tests','test','alaanmed.created')

consumer.consume(lambda x: print(x), queue_name='test', auto_ack=True)