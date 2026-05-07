package com.itsaur.event.kafka;

import com.itsaur.event.DomainEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.serializer.Deserializer;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class KafkaDomainEventConsumer {
    private final Logger log = LoggerFactory.getLogger(KafkaDomainEventConsumer.class);
    private final KafkaReceiver<String, byte[]> receiver;
    private final List<DomainEventListener<?>> listeners;
    private final Map<String, Deserializer<? extends DomainEvent>> deserializers;

    public KafkaDomainEventConsumer(
            KafkaReceiver<String, byte[]> receiver,
            List<DomainEventListener<?>> listeners,
            Map<String, Deserializer<? extends DomainEvent>> deserializers)
    {
        this.receiver = receiver;
        this.listeners = listeners;
        this.deserializers = deserializers;
    }

    public void startConsuming(){
        receiver.receive()
                .concatMap(this::processRecord)
                .subscribe();
    }

    private Mono<Void> processRecord(ReceiverRecord<String, byte[]> record){
        Header header = record.headers().lastHeader("eventType");

        String eventType = header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;

        Deserializer<? extends DomainEvent> deserializer = deserializers.get(eventType);
        DomainEvent event = deserializer.deserialize(record.value());

        return Flux.fromIterable(listeners)
                .filter(l -> l.eventType().isInstance(event))
                .concatMap(l -> {
                    @SuppressWarnings("unchecked")
                    DomainEventListener<DomainEvent> listener = (DomainEventListener<DomainEvent>) l;
                    return listener.handle(event)
                            .doOnError(e -> log.error("Listener {} failed for event {}",
                                    listener.getClass().getSimpleName(), event, e))
                            .onErrorResume(e -> Mono.empty());
                })
                .then(record.receiverOffset().commit());
    }
}