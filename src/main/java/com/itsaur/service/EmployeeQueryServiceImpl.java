package com.itsaur.service;

import com.itsaur.dto.ComplexSearchRequestDTO;
import com.itsaur.dto.PageDTO;
import com.itsaur.dto.SearchComponent;
import com.itsaur.dto.SimpleSearchRequestDTO;
import com.itsaur.exceptions.*;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.EmployeeQueryRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class EmployeeQueryServiceImpl implements EmployeeQueryService {

    private final EmployeeQueryRepository employeeQueryRepository;

    public EmployeeQueryServiceImpl(EmployeeQueryRepository employeeQueryRepository) {
        this.employeeQueryRepository = employeeQueryRepository;
    }

    @Override
    public Flux<EmployeeQueryModel> findAllEmployees() {
        return employeeQueryRepository.getAllEmployees();
    }

    @Override
    public Mono<EmployeeQueryModel> findEmployeeById(Long id) {
        if (id == 0 || id < 0) {
            return Mono.error(new EntityInvalidArgumentException("Id can not be null or empty"));
        }
        return employeeQueryRepository.getEmployeeById(id)
                .switchIfEmpty(Mono.error(new EmployeeNotFoundException("Employee with id: " + id + " not found")));
    }

    @Override
    public Mono<PageDTO<EmployeeQueryModel>> getPage(int page, int size) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        return Mono.zip(
                employeeQueryRepository.findPage(page, size).collectList(), employeeQueryRepository.countAll()
        ).map(tuple -> {
            var content = tuple.getT1();
            var total = tuple.getT2();
            return new PageDTO<>(content, page, size, total);
        });
    }

    @Override
    public Mono<PageDTO<EmployeeQueryModel>> getPageByName(int page, int size, String firstName) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        if(!firstName.matches("[A-Za-z]+")){
            return Mono.error(new FirstnameInvalidCharactersException("Name must contain alphabetic characters"));
        }

        return Mono.zip(employeeQueryRepository.findPageByFirstName(page, size, firstName).collectList(),
                employeeQueryRepository.countAllByFirstName(firstName)).map(tuple -> {
                    var content = tuple.getT1();
                    var total = tuple.getT2();
                    return new PageDTO<>(content, page, size, total);
        });
    }

    @Override
    public Mono<PageDTO<EmployeeQueryModel>> getPageByLastName(int page, int size, String lastName) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        if(!lastName.matches("[A-Za-z]+")){
            return Mono.error(new LastnameInvalidCharactersException("Lastname must contain alphabetic characters"));
        }

        return Mono.zip(employeeQueryRepository.findPageByLastName(page, size, lastName).collectList(),
                employeeQueryRepository.countAllByLastName(lastName)).map(tuple -> {
            var content = tuple.getT1();
            var total = tuple.getT2();
            return new PageDTO<>(content, page, size, total);
        });
    }

    @Override
    public Flux<EmployeeQueryModel> searchEmployees(SearchComponent searchComponent) {
        return employeeQueryRepository.filterQuery(searchComponent);
    }

    @Override
    public Mono<PageDTO<EmployeeQueryModel>> getPaginatedSearchEmployees(SearchComponent searchComponent, int page, int size) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        if (searchComponent instanceof SimpleSearchRequestDTO simpleSearchRequestDTO){
            String value = simpleSearchRequestDTO.getValue();

            if (value == null || value.trim().isEmpty()) {
                return Mono.error(new SearchValueEmptyException("Search value can not be empty"));
            }

            if (value.length() > 50) {
                return Mono.error(new SearchValueCharacterLengthException("Value must be 1 - 50 letters max."));
            }
        } else if (searchComponent instanceof ComplexSearchRequestDTO complexRequestDto) {
            if (complexRequestDto.getConditions() == null || complexRequestDto.getConditions().isEmpty()){
                return Mono.error(new EntityInvalidArgumentException("Conditions can not be empty"));
            }
            for (SearchComponent child: complexRequestDto.getConditions()){
                if (child instanceof SimpleSearchRequestDTO simpleChild){
                    String value = simpleChild.getValue();
                    if (value == null || value.trim().isEmpty()) {
                        return Mono.error(new SearchValueEmptyException("Search value can not be empty"));
                    }

                    if (value.length() > 50) {
                        return Mono.error(new SearchValueCharacterLengthException("Value must be 1 - 50 letters max."));
                    }
                }
            }
        }

        if (searchComponent == null){
            return Mono.zip(
                    employeeQueryRepository.findPage(page, size).collectList(), employeeQueryRepository.countAll()
            ).map(tuple -> {
                var content = tuple.getT1();
                var total = tuple.getT2();
                return new PageDTO<>(content, page, size, total);
            });
        } else {
            return Mono.zip(employeeQueryRepository.paginatedFilterQuery(searchComponent, page, size).collectList(), employeeQueryRepository.countViewEmployees(searchComponent))
                    .map(tuple -> {
                        var content = tuple.getT1();
                        var total = tuple.getT2();
                        return new PageDTO<>(content, page, size, total);
                    });
        }
    }
}
