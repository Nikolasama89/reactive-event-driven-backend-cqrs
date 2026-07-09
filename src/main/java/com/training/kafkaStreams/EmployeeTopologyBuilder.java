package com.training.kafkaStreams;

import com.training.event.*;
import com.training.model.Employee;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;


public class EmployeeTopologyBuilder {
    private static final String EVENTS_TOPIC = "domain-events";
    public static final String EMPLOYEES_STORE = "employees-store";
    public static final String EMAIL_EMPLOYEES_STORE = "email-employees-store";

    private final Serde<DomainEvent> eventSerde;
    private final Serde<Employee> employeeSerde;

    public EmployeeTopologyBuilder(Serde<DomainEvent> eventSerde, Serde<Employee> employeeSerde) {
        this.eventSerde = eventSerde;
        this.employeeSerde = employeeSerde;
    }

    public Topology build(){

        StreamsBuilder builder = new StreamsBuilder();

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EMPLOYEES_STORE),
                        Serdes.String(),
                        employeeSerde
                )
        );

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EMAIL_EMPLOYEES_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );

        builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), eventSerde))
                .filter((key, event) -> event != null && event.getClass().getSimpleName().contains("Employee"))
                .process(EmployeeProcessor::new, EMPLOYEES_STORE, EMAIL_EMPLOYEES_STORE);

        return builder.build();
    }
}
