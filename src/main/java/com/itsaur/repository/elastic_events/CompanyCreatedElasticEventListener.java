package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.repository.CompanyRepository;
import reactor.core.publisher.Mono;

public class CompanyCreatedElasticEventListener implements DomainEventListener<CompanyCreatedEvent> {

    private final CompanyRepository companyRepository;
    private final ElasticsearchAsyncClient client;

    public CompanyCreatedElasticEventListener(CompanyRepository companyRepository, ElasticsearchAsyncClient client) {
        this.companyRepository = companyRepository;
        this.client = client;
    }

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        return companyRepository.getCompanyById(event.getCompanyId())
                .flatMap(company -> {

                    CompanyQueryModel qm = new CompanyQueryModel(
                            company.getId(),
                            company.getCompanyName(),
                            company.getAddress(),
                            company.getVat(),
                            0L,
                            company.getCreatedAt(),
                            company.getUpdatedAt()
                    );

                    return Mono.fromFuture(() ->
                            client.index(i -> i
                                    .index("companies")
                                    .id(String.valueOf(company.getId()))
                                    .document(qm)
                            )
                    );
                })
                .then();
    }
}
