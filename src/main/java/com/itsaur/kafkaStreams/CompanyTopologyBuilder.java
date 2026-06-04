package com.itsaur.kafkaStreams;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEvent;
import com.itsaur.model.Company;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class CompanyTopologyBuilder {

    private static final String EVENTS_TOPIC = "domain-events";
    public static final String COMPANIES_STORE = "companies-store";
    public static final String VAT_COMPANIES_STORE = "vat-companies-store";

    private final Serde<DomainEvent> eventSerde;
    private final Serde<Company> companySerde;

    public CompanyTopologyBuilder(Serde<DomainEvent> eventSerde, Serde<Company> companySerde) {
        this.eventSerde = eventSerde;
        this.companySerde = companySerde;
    }

    public Topology build() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, DomainEvent> eventStream = builder
                .stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), eventSerde));

        KTable<String, Company> companiesTable = eventStream.groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .aggregate(() -> null, (id, event, current) ->
                                applyEvent(current, event),
                        Materialized.<String, Company, KeyValueStore<Bytes, byte[]>>as(COMPANIES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(companySerde));
        companiesTable
                .groupBy((id, company) -> KeyValue.pair(company.getVat().getValue(), id),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(() -> null,
                        (vat, currentId, aggregate) -> currentId,
                        (vat, oldId, aggregate) -> null,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(VAT_COMPANIES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));
        return builder.build();
    }

    private Company applyEvent(Company current, DomainEvent event) {
        if (event instanceof CompanyCreatedEvent e) {
            if (current != null) {
                return current;
            }
            return Company.construct(
                    e.getCompanyId().getValue(),
                    e.getCompanyName().getValue(),
                    e.getVat().getValue(),
                    e.getAddress().getValue(),
                    e.getOccurredAt(),
                    e.getOccurredAt()
            );
        }
        if (event instanceof CompanyUpdatedEvent e) {

            if (current == null) {
                return null;
            }
            return current.update(
                    e.getCompanyName().getValue(),
                    e.getVat().getValue(),
                    e.getAddress().getValue()
            );
        }
        if (event instanceof CompanyDeletedEvent) {
            return null;
        }
        return current;
    }
}
