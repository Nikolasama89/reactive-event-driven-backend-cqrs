package com.training.elastic.event;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;

public class IndexAddEmployeeNameToCompanyElasticEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final ElasticsearchAsyncClient client;

    public IndexAddEmployeeNameToCompanyElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {

        Map<String, Object> employee = Map.of(
                "id", event.getEmployeeId().getValue(),
                "firstname", event.getFullName().getFirstname(),
                "lastname", event.getFullName().getLastname());

        Map<String, JsonData> employeeParams = Map.of("employee", JsonData.of(employee));

        Script script = new Script.Builder()
                .inline(i -> i
                        .source("""
                                    if (ctx._source.employees == null) {
                                        ctx._source.employees = [];
                                    }
                                    ctx._source.employees.add(params.employee);
                                """)
                        .params(employeeParams)
                )
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