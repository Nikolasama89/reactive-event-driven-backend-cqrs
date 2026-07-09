package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

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
                        )), Object.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}