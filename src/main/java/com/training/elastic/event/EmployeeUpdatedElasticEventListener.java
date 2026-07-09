package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeUpdatedEvent;
import com.training.model.EmployeeQueryModel;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class EmployeeUpdatedElasticEventListener implements DomainEventListener<EmployeeUpdatedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeUpdatedElasticEventListener(ElasticsearchAsyncClient client) {

        this.client = client;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        EmployeeQueryModel queryModel =  new EmployeeQueryModel(
                event.getEmployeeId().getValue(),
                event.getFullName().getFirstname(),
                event.getFullName().getLastname(),
                event.getEmail().getValue(),
                event.getCompanyId().getValue(),
                event.getCompanyName().getValue(),
                event.getCompanyAddress().getValue(),
                event.getOccurredAt(),
                OffsetDateTime.now()
        );

        return Mono.fromFuture(() ->
                        client.update(u -> u
                                .index("employees")
                                .id(queryModel.getId().toString())
                                .doc(Map.of(
                                        "firstname", event.getFullName().getFirstname(),
                                        "lastname", event.getFullName().getLastname(),
                                        "updatedAt", event.getOccurredAt().truncatedTo(ChronoUnit.SECONDS).toString()
                                )), EmployeeQueryModel.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}