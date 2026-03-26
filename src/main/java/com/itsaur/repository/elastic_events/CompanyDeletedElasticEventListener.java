package com.itsaur.repository.elastic_events;

import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.repository.ElasticCompanyRepository;
import reactor.core.publisher.Mono;

public class CompanyDeletedElasticEventListener implements DomainEventListener<CompanyDeletedEvent> {

    private final ElasticCompanyRepository elasticRepository;

    public CompanyDeletedElasticEventListener(ElasticCompanyRepository elasticRepository) {
        this.elasticRepository = elasticRepository;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return elasticRepository.deleteCompany(event.getCompanyId());
    }
}
