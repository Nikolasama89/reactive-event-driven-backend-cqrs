package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.repository.CompanyRepository;
import reactor.core.publisher.Mono;

public class CompanyDeletedElasticEventListener implements DomainEventListener<CompanyDeletedEvent> {

    private final CompanyRepository companyRepository;
    private final ElasticsearchAsyncClient client;

    public CompanyDeletedElasticEventListener(CompanyRepository companyRepository, ElasticsearchAsyncClient client) {
        this.companyRepository = companyRepository;
        this.client = client;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return companyRepository.deleteCompany(event.getCompanyId())
                .then(Mono.fromFuture(client.delete(i -> i
                        .index("companies")
                        .id(String.valueOf(event.getCompanyId()))
                )))
                .then();
    }
}
