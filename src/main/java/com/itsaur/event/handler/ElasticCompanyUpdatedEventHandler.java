package com.itsaur.event.handler;

import com.itsaur.elastic.event.CompanyUpdatedElasticEventListener;
import com.itsaur.elastic.event.EmployeeAfterCompanyUpdatedElasticEventListener;
import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticCompanyUpdatedEventHandler implements DomainEventListener<CompanyUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticCompanyUpdatedEventHandler.class);
    private final CompanyUpdatedElasticEventListener companyUpdatedElasticEventListener;
    private final EmployeeAfterCompanyUpdatedElasticEventListener employeeAfterCompanyUpdatedElasticEventListener;

    public ElasticCompanyUpdatedEventHandler(
            CompanyUpdatedElasticEventListener companyUpdatedElasticEventListener,
            EmployeeAfterCompanyUpdatedElasticEventListener employeeAfterCompanyUpdatedElasticEventListener
    ) {
        this.companyUpdatedElasticEventListener = companyUpdatedElasticEventListener;
        this.employeeAfterCompanyUpdatedElasticEventListener = employeeAfterCompanyUpdatedElasticEventListener;
    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return companyUpdatedElasticEventListener.handle(event)
                .then(employeeAfterCompanyUpdatedElasticEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Company with id {} updated in Elastic successfully", event.aggregateId()))
                .doOnError(e -> logger.info("Elastic Failed to update company with id: {}, {}", event.aggregateId(), e.getMessage()));

    }
}