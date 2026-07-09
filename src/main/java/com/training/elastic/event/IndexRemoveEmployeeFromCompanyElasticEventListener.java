package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeDeletedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

public class IndexRemoveEmployeeFromCompanyElasticEventListener implements DomainEventListener<EmployeeDeletedEvent> {

    private final ElasticsearchAsyncClient client;

    public IndexRemoveEmployeeFromCompanyElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        Map<String, JsonData> params = Map.of("id", JsonData.of(event.getEmployeeId()));

        Script script = new Script.Builder().inline(i -> i.source("""
                if (ctx._source.employees != null) {
                    ctx._source.employees.removeIf(emp -> emp.id.equals(params.id));
                }
                """)
                        .params(params))
                .build();

        UpdateRequest<Object, Object> request = new UpdateRequest.Builder<>()
                .index("companies")
                .id(event.getCompanyId().getValue().toString())
                .script(script)
                .retryOnConflict(3)
                .build();

        return Mono.fromFuture(client.update(request, Object.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}