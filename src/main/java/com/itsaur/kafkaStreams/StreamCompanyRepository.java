package com.itsaur.kafkaStreams;

import com.itsaur.model.Company;
import com.itsaur.repository.CompanyRepository;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class StreamCompanyRepository implements CompanyRepository {
    private final KafkaStreams kafkaStreams;

    public StreamCompanyRepository(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public Mono<Company> getCompanyById(Long id) {
        String companyId = Long.toString(id);
        return Mono.fromCallable(() -> companiesStore().get(companyId))
                .subscribeOn(Schedulers.boundedElastic());
    }
    @Override
    public Mono<Boolean> existsVat(String vat) {
        return Mono.fromCallable(() -> vatIndexStore().get(vat))
                .subscribeOn(Schedulers.boundedElastic()).hasElement();
    }

    private ReadOnlyKeyValueStore<String, Company> companiesStore(){
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                CompanyTopologyBuilder.COMPANIES_STORE, QueryableStoreTypes.keyValueStore()));
    }

    private ReadOnlyKeyValueStore<String, String> vatIndexStore(){
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                CompanyTopologyBuilder.VAT_COMPANIES_STORE, QueryableStoreTypes.keyValueStore()));
    }
}