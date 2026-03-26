package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeUpdatedEvent;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.EmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeUpdatedElasticEventListener implements DomainEventListener<EmployeeUpdatedEvent> {

    private final EmployeeRepository employeeRepository;
    private final CompanyRepository companyRepository;
    private final ElasticsearchAsyncClient client;

    public EmployeeUpdatedElasticEventListener(EmployeeRepository employeeRepository, CompanyRepository companyRepository, ElasticsearchAsyncClient client) {
        this.employeeRepository = employeeRepository;
        this.companyRepository = companyRepository;
        this.client = client;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        return employeeRepository
                .getEmployeeById(event.getEmployeeId())
                .flatMap(employee -> {

                    Mono<EmployeeQueryModel> queryModel = companyRepository.getCompanyById(employee.getCompanyId())
                            .map(company -> new EmployeeQueryModel(
                                    employee.getId(),
                                    employee.getFirstname(),
                                    employee.getLastname(),
                                    employee.getEmail(),
                                    employee.getCompanyId(),
                                    company.getCompanyName(),
                                    company.getAddress(),
                                    employee.getCreatedAt(),
                                    employee.getUpdatedAt()
                            ));

                    return queryModel.flatMap(qm -> Mono.fromFuture(() ->
                            client.index(i -> i
                                    .index("employees")
                                    .id(qm.getId().toString())
                                    .document(qm)
                            )
                    ).thenReturn(employee));
                })
                .then();
    }
}
