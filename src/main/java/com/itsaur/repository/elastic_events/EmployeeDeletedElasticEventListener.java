package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeDeletedEvent;
import com.itsaur.repository.EmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeDeletedElasticEventListener implements DomainEventListener<EmployeeDeletedEvent> {

    private final EmployeeRepository employeeRepository;
    private final ElasticsearchAsyncClient client;

    public EmployeeDeletedElasticEventListener(EmployeeRepository employeeRepository, ElasticsearchAsyncClient client) {
        this.employeeRepository = employeeRepository;
        this.client = client;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return employeeRepository.deleteEmployee(event.getEmployeeId())
                .then(Mono.fromFuture(client.delete(i -> i
                        .index("employees")
                        .id(String.valueOf(event.getEmployeeId())))))
                .then();
    }
}
