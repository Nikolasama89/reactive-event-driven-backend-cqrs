package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import com.training.model.EmployeeQueryModel;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class EmployeeCreatedElasticEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeCreatedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {

        EmployeeQueryModel queryModel =  new EmployeeQueryModel(
                event.getEmployeeId().getValue(),
                event.getFullName().getFirstname(),
                event.getFullName().getLastname(),
                event.getEmail().getValue(),
                event.getCompanyId().getValue(),
                event.getCompanyName().getValue(),
                event.getCompanyAddress().getValue(),
                event.getOccurredAt(),
                event.getOccurredAt()
        );

        return Mono.fromFuture(() ->
                        client.index(i -> i
                                .index("employees")
                                .id(String.valueOf(queryModel.getId()))
                                .document(queryModel)
                        ))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100)).jitter(0.3))
                .then();
    }
}