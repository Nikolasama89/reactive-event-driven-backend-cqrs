package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.event.CompanyUpdatedEvent;
import com.training.event.DomainEventListener;
import com.training.model.CompanyQueryModel;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class CompanyUpdatedElasticEventListener implements DomainEventListener<CompanyUpdatedEvent> {

    private final ElasticsearchAsyncClient client;

    public CompanyUpdatedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;

    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return Mono.fromFuture(() ->
                        client.update(u -> u
                                .index("companies")
                                .id(String.valueOf(event.getCompanyId().getValue()))
                                .doc(Map.of(
                                        "companyName", event.getCompanyName().getValue(),
                                        "address", event.getAddress().getValue(),
                                        "vat", event.getVat().getValue(),
                                        "updatedAt", event.getOccurredAt().truncatedTo(ChronoUnit.SECONDS).toString()
                                )), CompanyQueryModel.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}