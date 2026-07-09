package com.training.config;

import com.training.event.*;
import com.training.serializer.*;

import java.util.Map;

public class DomainEventSerdes {
    public static Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializers(){
        return Map.of(
                CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer(),
                CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer(),
                CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer(),
                EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer(),
                EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer(),
                EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer());
    }


    public static Map<String, Deserializer<? extends DomainEvent>> deserializers() {
        return Map.of(
                "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer());
    }
}
