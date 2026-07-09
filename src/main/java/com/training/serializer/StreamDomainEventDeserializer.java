package com.training.serializer;

import com.training.event.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class StreamDomainEventDeserializer implements Deserializer<DomainEvent> {
    private final Map<String, com.training.serializer.Deserializer<? extends DomainEvent>> deserializers = new HashMap<>();

    public StreamDomainEventDeserializer() {
        deserializers.put("CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer());
        deserializers.put("CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer());
        deserializers.put("CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer());
        deserializers.put("EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer());
        deserializers.put("EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer());
        deserializers.put("EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer());
    }

    @Override
    public DomainEvent deserialize(String topic, byte[] data) {
        throw new UnsupportedOperationException("Requires Kafka Headers");
    }

    @Override
    public DomainEvent deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }

        var typeHeader = headers.lastHeader("eventType");
        if (typeHeader == null) {
            throw new IllegalArgumentException("Missing eventType header");
        }
        String type = new String(typeHeader.value(), StandardCharsets.UTF_8);
        com.training.serializer.Deserializer<? extends DomainEvent> deserializer = deserializers.get(type);
        return deserializer.deserialize(data);
    }
}
