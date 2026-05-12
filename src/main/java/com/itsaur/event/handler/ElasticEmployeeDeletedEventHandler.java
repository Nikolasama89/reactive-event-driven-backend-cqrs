package com.itsaur.event.handler;

import com.itsaur.elastic.event.EmployeeDeletedDecrementTotalEmployeesListener;
import com.itsaur.elastic.event.EmployeeDeletedElasticEventListener;
import com.itsaur.elastic.event.IndexRemoveEmployeeFromCompanyElasticEventListener;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeDeletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class ElasticEmployeeDeletedEventHandler implements DomainEventListener<EmployeeDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(ElasticEmployeeDeletedEventHandler.class);
    private final EmployeeDeletedElasticEventListener employeeDeletedElasticEventListener;
    private final EmployeeDeletedDecrementTotalEmployeesListener employeeDeletedDecrementTotalEmployeesListener;
    private final IndexRemoveEmployeeFromCompanyElasticEventListener indexRemoveEmployeeFromCompanyElasticEventListener;

    public ElasticEmployeeDeletedEventHandler(
            EmployeeDeletedElasticEventListener employeeDeletedElasticEventListener,
            EmployeeDeletedDecrementTotalEmployeesListener employeeDeletedDecrementTotalEmployeesListener,
            IndexRemoveEmployeeFromCompanyElasticEventListener indexRemoveEmployeeFromCompanyElasticEventListener
    ) {
        this.employeeDeletedElasticEventListener = employeeDeletedElasticEventListener;
        this.employeeDeletedDecrementTotalEmployeesListener = employeeDeletedDecrementTotalEmployeesListener;
        this.indexRemoveEmployeeFromCompanyElasticEventListener = indexRemoveEmployeeFromCompanyElasticEventListener;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return employeeDeletedElasticEventListener.handle(event)
                .then(employeeDeletedDecrementTotalEmployeesListener.handle(event))
                .then(indexRemoveEmployeeFromCompanyElasticEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee with id: {} deleted successfully from Elastic", event.aggregateId()))
                .doOnError(e -> logger.error("Elastic failed to delete employee with id: {}, {}", event.aggregateId(), e.getMessage()));
    }
}
