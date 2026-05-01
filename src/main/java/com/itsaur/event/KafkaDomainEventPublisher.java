package com.itsaur.event;

import com.itsaur.serializer.Serializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;

public class KafkaDomainEventPublisher implements EventPublisher{
    private final KafkaSender<String, byte[]> sender;
    private final String topic;
    private final Serializer<DomainEvent> serializer;

    public KafkaDomainEventPublisher(
            KafkaSender<String, byte[]> sender,
            String topic,
            Serializer<DomainEvent> serializer
    ) {
        this.sender = sender;
        this.topic = topic;
        this.serializer = serializer;
    }

    @Override
    public Mono<Void> publish(DomainEvent event) {
        String eventType = event.getClass().getSimpleName();
        byte[] value = serializer.serialize(event);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, eventType, value);
        record.headers().add(new RecordHeader("eventType", eventType.getBytes(StandardCharsets.UTF_8)));

        SenderRecord<String, byte[], String> senderRecord = SenderRecord.create(record, eventType);
        return sender.send(Mono.just(senderRecord))
                .then();
    }
}
