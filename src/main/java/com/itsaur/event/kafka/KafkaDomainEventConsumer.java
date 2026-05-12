package com.itsaur.event.kafka;

import com.itsaur.event.*;
import com.itsaur.serializer.Deserializer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaDomainEventConsumer {
    private final Logger logger = LoggerFactory.getLogger(KafkaDomainEventConsumer.class);
    private final KafkaReceiver<String, byte[]> receiver;
    private final Map<Class<? extends DomainEvent>, DomainEventListener<?>> handlers;
    private final Map<String, Deserializer<? extends DomainEvent>> deserializers;

    public KafkaDomainEventConsumer(
            KafkaReceiver<String, byte[]> receiver,
            Map<Class<? extends DomainEvent>, DomainEventListener<?>> handlers,
            Map<String, Deserializer<? extends DomainEvent>> deserializers
    ) {
        this.receiver = receiver;
        this.handlers = handlers;
        this.deserializers = deserializers;
    }

    public void startConsuming(){
        receiver.receive()
                .concatMap(record -> processRecord(record)
                        .then(record.receiverOffset().commit()))
                .subscribe();
    }

    private Mono<Void> processRecord(ReceiverRecord<String, byte[]> record){

        Header header = record.headers().lastHeader("eventType");
        if (header == null){
            logger.warn("Not known event");
            return Mono.empty();
        }

        String eventType = new String(header.value(), StandardCharsets.UTF_8);

        Deserializer<? extends DomainEvent> deserializer = deserializers.get(eventType);
        if (deserializer == null) {
            logger.warn("No deserializer for this event");
            return Mono.empty();
        }

        DomainEvent event = deserializer.deserialize(record.value());

        @SuppressWarnings("unchecked")
        DomainEventListener<DomainEvent> handler = (DomainEventListener<DomainEvent>) handlers.get(event.getClass());

        if (handler == null){
            logger.warn("No handler for this event");
            return Mono.empty();
        }
        return handler.handle(event);
    }
}