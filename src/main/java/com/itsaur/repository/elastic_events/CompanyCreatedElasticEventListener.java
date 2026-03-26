package com.itsaur.repository.elastic_events;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.ElasticCompanyRepository;
import reactor.core.publisher.Mono;

public class CompanyCreatedElasticEventListener implements DomainEventListener<CompanyCreatedEvent> {

    private final CompanyRepository companyRepository;
    private final ElasticCompanyRepository elasticRepository;

    public CompanyCreatedElasticEventListener(CompanyRepository companyRepository, ElasticCompanyRepository elasticRepository) {
        this.companyRepository = companyRepository;
        this.elasticRepository = elasticRepository;
    }

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        return companyRepository.getCompanyById(event.getCompanyId())
                .flatMap(company -> elasticRepository.createOrUpdateCompany(company.getId(), company)).then();
    }
}
