package com.itsaur.kafkaStreams;

import com.itsaur.event.*;
import com.itsaur.model.Employee;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;



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

        KStream<String, DomainEvent> eventStream = builder
                .stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), eventSerde));

        KTable<String, Employee> employeesTable = eventStream.groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .aggregate(() -> null, (id, event, current) ->
                                applyEvent(current, event),
                        Materialized.<String, Employee, KeyValueStore<Bytes, byte[]>>as(EMPLOYEES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(employeeSerde));
        employeesTable
                .groupBy(
                        (id, employee) -> KeyValue.pair(employee.getEmail().getValue(), id),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(() -> null,
                        (email, currentId, aggregate) -> currentId,
                        (email, oldId, aggregate) -> null,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(EMAIL_EMPLOYEES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );
        return builder.build();
    }

    private Employee applyEvent(Employee current, DomainEvent event) {
        if (event instanceof EmployeeCreatedEvent e) {
            if (current != null) {
                return current;
            }
            return Employee.construct(
                    e.getEmployeeId().getValue(),
                    e.getFullName().getFirstname(),
                    e.getFullName().getLastname(),
                    e.getEmail().getValue(),
                    e.getCompanyId().getValue(),
                    e.getOccurredAt(),
                    e.getOccurredAt()
            );
        }
        if (event instanceof EmployeeUpdatedEvent e) {

            if (current == null) {
                return null;
            }
            return current.update(
                    e.getFullName().getFirstname(),
                    e.getFullName().getLastname()
            );
        }
        if (event instanceof EmployeeDeletedEvent) {
            return null;
        }
        return current;
    }
}
