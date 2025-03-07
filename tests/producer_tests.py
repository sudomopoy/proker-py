from proker.factory import MessageBrokerFactory

factory = MessageBrokerFactory()


producer = factory.get_producer("rabbitmq", {
    'exchange':{
        'name':'tests'
    }
})

producer.connect()
producer.publish("{\"name\":\"alaanmed\",\"age\":20}",'alaanmed.created')
