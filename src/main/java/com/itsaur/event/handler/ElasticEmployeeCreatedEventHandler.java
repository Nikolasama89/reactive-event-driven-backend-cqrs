package com.itsaur.event.handler;

import com.itsaur.elastic.event.EmployeeCreateIncrementTotalEmployeesListener;
import com.itsaur.elastic.event.EmployeeCreatedElasticEventListener;
import com.itsaur.elastic.event.IndexAddEmployeeNameToCompanyElasticEventListener;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticEmployeeCreatedEventHandler implements DomainEventListener<EmployeeCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticEmployeeCreatedEventHandler.class);
    private final EmployeeCreatedElasticEventListener employeeCreatedElasticEventListener;
    private final EmployeeCreateIncrementTotalEmployeesListener employeeCreateIncrementTotalEmployeesListener;
    private final IndexAddEmployeeNameToCompanyElasticEventListener indexAddEmployeeNameToCompanyElasticEventListener;

    public ElasticEmployeeCreatedEventHandler(
            EmployeeCreatedElasticEventListener employeeCreatedElasticEventListener,
            EmployeeCreateIncrementTotalEmployeesListener employeeCreateIncrementTotalEmployeesListener,
            IndexAddEmployeeNameToCompanyElasticEventListener indexAddEmployeeNameToCompanyElasticEventListener
    ) {
        this.employeeCreatedElasticEventListener = employeeCreatedElasticEventListener;
        this.employeeCreateIncrementTotalEmployeesListener = employeeCreateIncrementTotalEmployeesListener;
        this.indexAddEmployeeNameToCompanyElasticEventListener = indexAddEmployeeNameToCompanyElasticEventListener;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        return employeeCreatedElasticEventListener.handle(event)
                .then(employeeCreateIncrementTotalEmployeesListener.handle(event))
                .then(indexAddEmployeeNameToCompanyElasticEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee created in Elastic successfully with id {}", event.aggregateId()))
                .doOnError(e -> logger.error("Elastic Failed to create employee with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}
