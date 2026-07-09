package com.training.event.kafka;

import com.training.event.*;
import com.training.serializer.Deserializer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaDomainEventConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDomainEventConsumer.class);
    private final KafkaReceiver<String, byte[]> receiver;
    private final Map<Class<? extends DomainEvent>, List<DomainEventListener<?>>> listeners;
    private final Map<String, Deserializer<? extends DomainEvent>> deserializers;

    public KafkaDomainEventConsumer(
            KafkaReceiver<String, byte[]> receiver,
            List<DomainEventListener<?>> listenerList,
            Map<String, Deserializer<? extends DomainEvent>> deserializers
    ) {
        this.receiver = receiver;
        this.listeners = new HashMap<>();
        this.deserializers = deserializers;

        for (DomainEventListener<?> listener : listenerList){
//            if (!listeners.containsKey(listener.eventType())){
//                listeners.put((Class<? extends DomainEvent>) listener.eventType(), new ArrayList<>());
//            }
            listeners.computeIfAbsent((Class<? extends DomainEvent>) listener.eventType(),
                            k -> new ArrayList<>())
                    .add(listener);
//            listeners.get(listener.eventType()).add(listener);
        }
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
            LOGGER.warn("Not known event");
            return Mono.empty();
        }

        String eventType = new String(header.value(), StandardCharsets.UTF_8);

        Deserializer<? extends DomainEvent> deserializer = deserializers.get(eventType);
        if (deserializer == null) {
            LOGGER.warn("No deserializer for event: {}", eventType);
            return Mono.empty();
        }

        DomainEvent event = deserializer.deserialize(record.value());

        List<DomainEventListener<?>> eventListeners =  listeners.get(event.getClass());
        if (eventListeners == null){
            LOGGER.warn("No event listeners for {}", event.getClass().getName());
            return Mono.empty();
        }

        return Flux.fromIterable(eventListeners)
                .concatMap(l -> {
                    @SuppressWarnings("unchecked")
                    DomainEventListener<DomainEvent> listener = (DomainEventListener<DomainEvent>) l;
                    return listener.handle(event);
                })
                .then();
    }
}