package com.itsaur.event.handler;

import com.itsaur.elastic.event.CompanyDeletedElasticEventListener;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticCompanyDeletedEventHandler implements DomainEventListener<CompanyDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticCompanyDeletedEventHandler.class);
    private final CompanyDeletedElasticEventListener companyDeletedElasticEventListener;

    public ElasticCompanyDeletedEventHandler(CompanyDeletedElasticEventListener companyDeletedElasticEventListener) {
        this.companyDeletedElasticEventListener = companyDeletedElasticEventListener;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return companyDeletedElasticEventListener.handle(event)
                .doOnSuccess(s -> logger.info("Company with id: {} deleted from Elastic successfully", event.aggregateId()))
                .doOnError(e -> logger.error("Elastic failed to delete company with id: {}, {}", event.aggregateId(), e.getMessage()));
    }
}