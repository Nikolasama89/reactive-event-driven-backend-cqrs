package com.itsaur.service;

import com.itsaur.dto.PageDTO;
import com.itsaur.dto.SearchComponent;
import com.itsaur.model.EmployeeQueryModel;
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
