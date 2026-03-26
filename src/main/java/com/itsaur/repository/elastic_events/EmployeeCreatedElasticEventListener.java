package com.itsaur.repository.elastic_events;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.repository.ElasticEmployeeRepository;
import com.itsaur.repository.EmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeCreatedElasticEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final EmployeeRepository employeeRepository;
    private final ElasticEmployeeRepository elasticEmployeeRepository;

    public EmployeeCreatedElasticEventListener(EmployeeRepository employeeRepository, ElasticEmployeeRepository elasticEmployeeRepository) {
        this.employeeRepository = employeeRepository;
        this.elasticEmployeeRepository = elasticEmployeeRepository;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        return employeeRepository
                .getEmployeeById(event.getEmployeeId())
                .flatMap(employee -> elasticEmployeeRepository.createOrUpdateEmployee(employee.getId(), employee))
                .then();
    }
}
