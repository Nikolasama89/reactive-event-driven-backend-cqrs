package com.itsaur.repository;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.itsaur.model.Employee;
import com.itsaur.model.EmployeeQueryModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class ElasticEmployeeRepository implements EmployeeRepository{

    private final CompanyRepository companyRepository;
    private final Logger logger = LoggerFactory.getLogger(ElasticEmployeeRepository.class );
    private final ElasticsearchAsyncClient client;

    public ElasticEmployeeRepository(ElasticsearchAsyncClient client, CompanyRepository companyRepository) {
        this.client = client;
        this.companyRepository = companyRepository;

    }
    @Override
    public Mono<Employee> getEmployeeById(Long id) {

//        return Mono.fromFuture(() -> client.get(g -> g.index("employees").id(String.valueOf(id)), Employee.class)
//        ).flatMap(response -> {
//            if (response.found()){
//                return Mono.just(response.source());
//            }
//            return Mono.empty();
//        });
        return Mono.fromFuture(() ->
                        client.get(g -> g.index("employees").id(String.valueOf(id)), EmployeeQueryModel.class)
                )
                .flatMap(response -> {
                    if (response.found()) {
                        EmployeeQueryModel qm = response.source();
                        Employee emp = new Employee(
                                qm.getId(),
                                qm.getFirstname(),
                                qm.getLastname(),
                                qm.getEmail(),
                                qm.getCompanyId(),
                                qm.getCreatedAt(),
                                qm.getUpdatedAt()
                        );
                        return Mono.just(emp);
                    } else {
                        return Mono.empty();
                    }
                });
    }

    @Override
    public Mono<Employee> createOrUpdateEmployee(Long id, Employee employee) {

        OffsetDateTime now = OffsetDateTime.now();

        if (id == null && employee.getId() == null) {
            employee.setId(System.currentTimeMillis());
            employee.setCreatedAt(now);
        }

        if (employee.getCreatedAt() == null) {
            employee.setCreatedAt(now);
        }

        employee.setUpdatedAt(now);

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
    }

    @Override
    public Mono<Void> deleteEmployee(Long id) {
        return Mono.fromFuture(() ->
                        client.delete(i -> i.index("employees").id(String.valueOf(id))))
                .then();
    }

    @Override
    public Mono<Boolean> existsEmail(String email) {

        if (email == null || email.isBlank()) {
            return Mono.just(false);
        }

        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .size(0)
                .query(q -> q
                        .term(t -> t
                                .field("email")
                                .value(v -> v.stringValue(email))
                        )
                )
        );

        return Mono.fromFuture(client.search(request, Employee.class))
                .map(resp -> resp.hits().total().value() > 0);
    }
}
