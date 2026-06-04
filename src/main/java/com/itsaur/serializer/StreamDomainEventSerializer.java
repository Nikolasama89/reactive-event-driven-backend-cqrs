package com.itsaur.serializer;

import com.itsaur.event.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class StreamDomainEventSerializer implements Serializer<DomainEvent> {

    private final Map<Class <? extends DomainEvent>, com.itsaur.serializer.Serializer<? extends DomainEvent>> serializers = new HashMap<>();

    public StreamDomainEventSerializer() {
        serializers.put(CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer());
        serializers.put(CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer());
        serializers.put(CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer());
        serializers.put(EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer());
        serializers.put(EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer());
        serializers.put(EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer());
    }

    @Override
    public byte[] serialize(String topic, DomainEvent event) {
        throw new UnsupportedOperationException("Requires Kafka Headers");
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(String topic, Headers headers, DomainEvent event) {
        if (event == null) {
            return null;
        }

        com.itsaur.serializer.Serializer<DomainEvent> serializer = (com.itsaur.serializer.Serializer<DomainEvent>) serializers.get(event.getClass());
        if (serializer == null) {
            throw new IllegalArgumentException("Unknown DomainEvent type");
        }

        String eventType = event.getClass().getSimpleName();
        if (headers != null) {
            headers.add(new RecordHeader("eventType", eventType.getBytes(StandardCharsets.UTF_8)));
        }
        return serializer.serialize(event);
    }
}
