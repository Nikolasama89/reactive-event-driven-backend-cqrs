package com.itsaur.repository.elastic_events;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.EmployeeRepository;
import reactor.core.publisher.Mono;

public class EmployeeCreatedElasticEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final EmployeeRepository employeeRepository;
    private final ElasticsearchAsyncClient client;
    private final CompanyRepository companyRepository;

    public EmployeeCreatedElasticEventListener(EmployeeRepository employeeRepository, ElasticsearchAsyncClient client, CompanyRepository companyRepository) {
        this.employeeRepository = employeeRepository;
        this.client = client;
        this.companyRepository = companyRepository;

    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
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
