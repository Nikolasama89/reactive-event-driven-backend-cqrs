package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.CompanyCreatedEvent;
import com.training.event.DomainEventListener;
import com.training.model.CompanyQueryModel;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class CompanyCreatedElasticEventListener implements DomainEventListener<CompanyCreatedEvent> {

    private final ElasticsearchAsyncClient client;

    public CompanyCreatedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        CompanyQueryModel qm = new CompanyQueryModel(
                event.getCompanyId().getValue(),
                event.getCompanyName().getValue(),
                event.getVat().getValue(),
                event.getAddress().getValue(),
                0L,
                event.getOccurredAt(),
                event.getOccurredAt()
        );

        return Mono.fromFuture(() ->
                        client.index(i -> i
                                .index("companies")
                                .id(String.valueOf(qm.getId()))
                                .document(qm)))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}