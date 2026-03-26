package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import reactor.core.publisher.Mono;

public class EmployeeCreateIncrementTotalEmployeesListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeCreateIncrementTotalEmployeesListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        return Mono.fromFuture(() -> client.update(u -> u
                        .index("companies")
                        .id(String.valueOf(event.getCompanyId()))
                        .script(s -> s.inline(i -> i
                                .source("ctx._source.total_employees += 1")
                        )),
                Object.class
        )).then();
    }
}
