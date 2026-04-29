package com.itsaur.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class CompanyDeletedElasticEventListener implements DomainEventListener<CompanyDeletedEvent> {

    private final ElasticsearchAsyncClient client;

    public CompanyDeletedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return Mono.fromFuture(() -> client.delete(i -> i
                        .index("companies")
                        .id(String.valueOf(event.getCompanyId().getValue()))))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100)).jitter(0.3))
                .then();
    }
}