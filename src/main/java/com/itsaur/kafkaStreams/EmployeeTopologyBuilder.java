package com.itsaur.kafkaStreams;

import com.itsaur.event.*;
import com.itsaur.model.Employee;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
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
        Topology topology = new Topology();

        topology.addSource(
                "EmployeeEventSource",
                Serdes.String().deserializer(),
                eventSerde.deserializer(),
                EVENTS_TOPIC
        );

        topology.addProcessor(
                "EmployeeProcessorNode",
                EmployeeProcessor::new,
                "EmployeeEventSource"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EMPLOYEES_STORE),
                        Serdes.String(),
                        employeeSerde
                ),
                "EmployeeProcessorNode"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(EMAIL_EMPLOYEES_STORE),
                        Serdes.String(),
                        Serdes.String()
                ),
                "EmployeeProcessorNode"
        );
        return topology;
    }
}
