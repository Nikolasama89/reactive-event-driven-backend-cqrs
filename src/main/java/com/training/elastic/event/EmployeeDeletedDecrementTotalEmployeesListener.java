package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeDeletedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

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
                        )), Object.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}