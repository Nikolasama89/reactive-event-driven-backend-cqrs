package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.JsonData;
import com.training.event.CompanyUpdatedEvent;
import com.training.event.DomainEventListener;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

public class EmployeeAfterCompanyUpdatedElasticEventListener implements DomainEventListener<CompanyUpdatedEvent> {

    private final ElasticsearchAsyncClient client;

    public EmployeeAfterCompanyUpdatedElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return Mono.fromFuture(client.updateByQuery(u -> u
                        .index("employees")
                        .query(q -> q.term(t -> t
                                .field("companyId")
                                .value(event.getCompanyId().getValue())))
                        .script(s -> s.inline(i -> i
                                .source("""
                            ctx._source.companyName = params.companyName;
                            ctx._source.companyAddress = params.companyAddress;
                        """)
                                .params(Map.of(
                                        "companyName", JsonData.of(event.getCompanyName().getValue()),
                                        "companyAddress", JsonData.of(event.getAddress().getValue())))))))
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(10))
                        .jitter(0.3))
                .then();
    }
}