package com.training.service;

import com.training.dto.PageDTO;
import com.training.repository.search.SearchComponent;
import com.training.model.EmployeeQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EmployeeQueryService {
    Flux<EmployeeQueryModel> findAllEmployees();
    Mono<EmployeeQueryModel> findEmployeeById(Long id);
    Mono<PageDTO<EmployeeQueryModel>> getPage(int page, int size);
    Mono<PageDTO<EmployeeQueryModel>> getPageByName(int page, int size, String firstName);
    Mono<PageDTO<EmployeeQueryModel>> getPageByLastName(int page, int size, String lastName);
    Flux<EmployeeQueryModel> searchEmployees(SearchComponent searchComponent);
    Mono<PageDTO<EmployeeQueryModel>> getPaginatedSearchEmployees(SearchComponent searchComponent, int page, int size);
}
