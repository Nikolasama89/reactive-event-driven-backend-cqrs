package com.itsaur.event.handler;

import com.itsaur.elastic.event.CompanyCreatedElasticEventListener;
import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.DomainEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticCompanyCreatedEventHandler implements DomainEventListener<CompanyCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticCompanyCreatedEventHandler.class);
    private final CompanyCreatedElasticEventListener elasticEventListener;

    public ElasticCompanyCreatedEventHandler(CompanyCreatedElasticEventListener elasticEventListener) {
        this.elasticEventListener = elasticEventListener;
    }


    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        return elasticEventListener.handle(event)
                .doOnSuccess(s -> logger.info("Company created in Elastic successfully with id: {}", event.aggregateId()))
                .doOnError(e -> logger.error("Elastic failed to create company with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}
