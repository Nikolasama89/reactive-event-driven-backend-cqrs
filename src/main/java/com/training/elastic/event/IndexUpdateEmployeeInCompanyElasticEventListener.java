package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeUpdatedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

public class IndexUpdateEmployeeInCompanyElasticEventListener implements DomainEventListener<EmployeeUpdatedEvent> {

    private final ElasticsearchAsyncClient client;

    public IndexUpdateEmployeeInCompanyElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {

        Map<String, Object> employee = Map.of(
                "id", event.getEmployeeId().getValue(),
                "firstname", event.getFullName().getFirstname(),
                "lastname", event.getFullName().getLastname()
        );

        Map<String, JsonData> params = Map.of(
                "id", JsonData.of(event.getEmployeeId().getValue()),
                "employee", JsonData.of(employee)
        );

        Script script = new Script.Builder()
                .inline(i -> i.source("""
                        if (ctx._source.employees != null) {
                            for (int i = 0; i < ctx._source.employees.size(); i++) {
                                if (ctx._source.employees[i].id.equals(params.id)) {
                                    ctx._source.employees[i] = params.employee;
                                }
                            }
                        }
                        """).params(params))
                .build();

        UpdateRequest<Object, Object> request = new UpdateRequest.Builder<>()
                .index("companies")
                .id(event.getCompanyId().getValue().toString())
                .script(script)
                .build();

        return Mono.fromFuture(client.update(request, Object.class))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .jitter(0.3))
                .then();
    }
}