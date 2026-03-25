package com.itsaur.event;

import com.itsaur.repository.ElasticEmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeDeletedElasticEventListener implements DomainEventListener<EmployeeDeletedEvent>{

    private final ElasticEmployeeRepository elasticEmployeeRepository;

    public EmployeeDeletedElasticEventListener(ElasticEmployeeRepository elasticEmployeeRepository) {
        this.elasticEmployeeRepository = elasticEmployeeRepository;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return elasticEmployeeRepository.deleteEmployee(event.getEmployeeId());
    }
}
