package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeDeletedEvent;
import reactor.core.publisher.Mono;

public class EmployeeDeletedDecrementTotalEmployeesListener implements DomainEventListener<EmployeeDeletedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeDeletedDecrementTotalEmployeesListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return Mono.fromFuture(() -> client.update(u -> u
                        .index("companies")
                        .id(String.valueOf(event.getCompanyId()))
                        .script(s -> s.inline(i -> i
                                .source("ctx._source.total_employees -= 1")
                        )),
                Object.class
        )).then();
    }
}
