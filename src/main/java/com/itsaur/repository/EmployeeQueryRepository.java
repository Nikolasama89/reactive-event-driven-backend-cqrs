package com.itsaur.repository;

import com.itsaur.repository.search.SearchComponent;
import com.itsaur.model.EmployeeQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EmployeeQueryRepository {
    Mono<EmployeeQueryModel> getEmployeeById(Long id);
    Flux<EmployeeQueryModel> getAllEmployees();
    Flux<EmployeeQueryModel> findPage(Integer page, Integer size);
    Mono<Long> countAll();
    Flux<EmployeeQueryModel> findPageByLastName(Integer page, Integer size, String lastName);
    Flux<EmployeeQueryModel> findPageByFirstName(Integer page, Integer size, String firstName);
    Mono<Long> countAllByFirstName(String firstName);
    Mono<Long> countAllByLastName(String lastName);
    Flux<EmployeeQueryModel> filterQuery(SearchComponent searchComponent);
    Flux<EmployeeQueryModel> paginatedFilterQuery(SearchComponent searchComponent, Integer page, Integer size);
    Mono<Long> countViewEmployees(SearchComponent searchComponent);
}
