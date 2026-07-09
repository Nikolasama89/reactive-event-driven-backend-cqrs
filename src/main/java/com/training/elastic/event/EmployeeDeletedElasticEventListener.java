package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeDeletedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class EmployeeDeletedElasticEventListener implements DomainEventListener<EmployeeDeletedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeDeletedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return Mono.fromFuture(() -> client.delete(i -> i
                        .index("employees")
                        .id(String.valueOf(event.getEmployeeId().getValue()))))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100)).jitter(0.3))
                .then();
    }
}
