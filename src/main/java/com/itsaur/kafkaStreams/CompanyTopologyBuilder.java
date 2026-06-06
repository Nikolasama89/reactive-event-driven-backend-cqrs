package com.itsaur.kafkaStreams;

import com.itsaur.event.DomainEvent;
import com.itsaur.model.Company;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
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

        Topology topology = new Topology();

        topology.addSource(
                "CompanyEventSource",
                Serdes.String().deserializer(),
                eventSerde.deserializer(),
                EVENTS_TOPIC
        );

        topology.addProcessor(
                "CompanyProcessorNode",
                CompanyProcessor::new,
                "CompanyEventSource"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(COMPANIES_STORE),
                        Serdes.String(),
                        companySerde
                ),
                "CompanyProcessorNode"
        );

        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(VAT_COMPANIES_STORE),
                        Serdes.String(),
                        Serdes.String()
                ),
                "CompanyProcessorNode"
        );
        return topology;
    }
}
