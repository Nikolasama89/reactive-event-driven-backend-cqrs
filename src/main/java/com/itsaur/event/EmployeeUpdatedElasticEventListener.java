package com.itsaur.event;

import com.itsaur.repository.ElasticEmployeeRepository;
import com.itsaur.repository.EmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeUpdatedElasticEventListener implements DomainEventListener<EmployeeUpdatedEvent>{

    private final EmployeeRepository employeeRepository;
    private final ElasticEmployeeRepository elasticEmployeeRepository;

    public EmployeeUpdatedElasticEventListener(EmployeeRepository employeeRepository, ElasticEmployeeRepository elasticEmployeeRepository) {
        this.employeeRepository = employeeRepository;
        this.elasticEmployeeRepository = elasticEmployeeRepository;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        return employeeRepository.getEmployeeById(event.getEmployeeId())
                .flatMap(updatedEmployee ->
                        elasticEmployeeRepository.createOrUpdateEmployee(updatedEmployee.getId(), updatedEmployee))
                .then();
    }
}
