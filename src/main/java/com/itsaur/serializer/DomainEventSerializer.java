package com.itsaur.serializer;

import com.itsaur.event.DomainEvent;

import java.util.Map;

public class DomainEventSerializer implements Serializer<DomainEvent> {

    private final Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializers;

    public DomainEventSerializer(Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializers) {
        this.serializers = serializers;
    }

    @Override
    public byte[] serialize(DomainEvent event) {
        Serializer serializer = serializers.get(event.getClass());

        if (serializer == null) {
            throw new RuntimeException("No serializer for " + event.getClass());
        }

        return serializer.serialize(event);
    }
}
