package com.itsaur.repository.elastic_events;

import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.ElasticCompanyRepository;
import reactor.core.publisher.Mono;

public class CompanyUpdatedElasticEventListener implements DomainEventListener<CompanyUpdatedEvent> {

    private final CompanyRepository companyRepository;
    private final ElasticCompanyRepository elasticRepository;

    public CompanyUpdatedElasticEventListener(CompanyRepository companyRepository, ElasticCompanyRepository elasticRepository) {
        this.companyRepository = companyRepository;
        this.elasticRepository = elasticRepository;
    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return companyRepository.getCompanyById(event.getCompanyId())
                .flatMap(updatedCompany ->
                        elasticRepository.createOrUpdateCompany(updatedCompany.getId(), updatedCompany)).then();
    }
}
