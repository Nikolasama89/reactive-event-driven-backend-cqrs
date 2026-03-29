package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import reactor.core.publisher.Mono;

import java.util.Map;

public class IndexEmployeeNameToCompanyElasticEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final ElasticsearchAsyncClient client;

    public IndexEmployeeNameToCompanyElasticEventListener(ElasticsearchAsyncClient client) {
        this.client = client;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {

        Map<String, Object> employee = Map.of("firstname", event.getFirstname(), "lastname", event.getLastname());

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

        UpdateRequest<Object, Object> request = new UpdateRequest.Builder<Object, Object>()
                .index("companies")
                .id(String.valueOf(event.getCompanyId()))
                .script(script)
                .build();

        return Mono.fromFuture(client.update(request, Object.class)).then();
    }
}
