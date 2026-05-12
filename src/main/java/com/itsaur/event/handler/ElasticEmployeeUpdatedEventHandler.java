package com.itsaur.event.handler;

import com.itsaur.elastic.event.EmployeeUpdatedElasticEventListener;
import com.itsaur.elastic.event.IndexUpdateEmployeeInCompanyElasticEventListener;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticEmployeeUpdatedEventHandler implements DomainEventListener<EmployeeUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticEmployeeUpdatedEventHandler.class);
    private final EmployeeUpdatedElasticEventListener employeeUpdatedElasticEventListener;
    private final IndexUpdateEmployeeInCompanyElasticEventListener indexUpdateEmployeeInCompanyElasticEventListener;

    public ElasticEmployeeUpdatedEventHandler(
            EmployeeUpdatedElasticEventListener employeeUpdatedElasticEventListener,
            IndexUpdateEmployeeInCompanyElasticEventListener indexUpdateEmployeeInCompanyElasticEventListener
    ) {
        this.employeeUpdatedElasticEventListener = employeeUpdatedElasticEventListener;
        this.indexUpdateEmployeeInCompanyElasticEventListener = indexUpdateEmployeeInCompanyElasticEventListener;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        return employeeUpdatedElasticEventListener.handle(event)
                .then(indexUpdateEmployeeInCompanyElasticEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee with id {} updated in Elastic successfully", event.aggregateId()))
                .doOnError(e -> logger.error("Elastic failed to update employee with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}
