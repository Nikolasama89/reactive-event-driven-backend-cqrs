package com.itsaur.kafkaStreams;

import com.itsaur.event.DomainEvent;
import com.itsaur.model.Company;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.Stores;

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

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(COMPANIES_STORE),
                        Serdes.String(),
                        companySerde
                )
        );

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(VAT_COMPANIES_STORE),
                        Serdes.String(),
                        Serdes.String()
                )
        );

        builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), eventSerde))
                .filter((key, event) -> event != null && event.getClass().getSimpleName().contains("Company"))
                .process(CompanyProcessor::new, COMPANIES_STORE, VAT_COMPANIES_STORE);

        return builder.build();
    }
}
