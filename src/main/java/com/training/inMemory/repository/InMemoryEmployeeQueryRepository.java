package com.training.inMemory.repository;

import com.training.repository.search.SearchComponent;
import com.training.model.Company;
import com.training.model.Employee;
import com.training.model.EmployeeQueryModel;
import com.training.repository.EmployeeQueryRepository;
import com.training.inMemory.search.InMemoryEmployeeSearch;
import com.training.inMemory.search.InMemoryStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;


public class InMemoryEmployeeQueryRepository implements EmployeeQueryRepository {

    private final InMemoryStorage inMemoryStorage;
    private final InMemoryEmployeeSearch searchCompiler;

    public InMemoryEmployeeQueryRepository(InMemoryStorage inMemoryStorage, InMemoryEmployeeSearch searchCompiler){
        this.inMemoryStorage = inMemoryStorage;
        this.searchCompiler = searchCompiler;
    }

    @Override
    public Mono<EmployeeQueryModel> getEmployeeById(Long id) {
        return Mono.justOrEmpty(inMemoryStorage.employees.get(id)).flatMap(this::toQueryModel);
    }

    @Override
    public Flux<EmployeeQueryModel> getAllEmployees() {
        return Flux.fromIterable(inMemoryStorage.getEmployees().values())
                .flatMap(this::toQueryModel);
    }

    @Override
    public Flux<EmployeeQueryModel> findPage(Integer page, Integer size) {

        long offset = (long) page * size;

        return getAllEmployees()
                .skip(offset)
                .take(size);

    }

    @Override
    public Mono<Long> countAll() {
        return getAllEmployees().count();
    }

    @Override
    public Flux<EmployeeQueryModel> findPageByLastName(Integer page, Integer size, String lastName) {
        long offset = (long) page * size;

        return getAllEmployees()
                .filter(employee ->
                        employee.getLastname() != null && employee.getLastname().toLowerCase().contains(lastName.toLowerCase()))
                .skip(offset)
                .take(size);
    }

    @Override
    public Flux<EmployeeQueryModel> findPageByFirstName(Integer page, Integer size, String firstName) {
        long offset = (long) page * size;

        return getAllEmployees()
                .filter(employee ->
                        employee.getFirstname() != null && employee.getFirstname().toLowerCase().contains(firstName.toLowerCase()))
                .skip(offset)
                .take(size);
    }

    @Override
    public Mono<Long> countAllByFirstName(String firstName) {
        return getAllEmployees()
                .filter(employee ->
                        employee.getFirstname() != null && employee.getFirstname().toLowerCase().contains(firstName.toLowerCase()))
                .count();
    }

    @Override
    public Mono<Long> countAllByLastName(String lastName) {
        return getAllEmployees()
                .filter(employee ->
                        employee.getLastname() != null && employee.getLastname().toLowerCase().contains(lastName.toLowerCase()))
                .count();
    }

    @Override
    public Flux<EmployeeQueryModel> filterQuery(SearchComponent searchComponent) {
        Predicate<EmployeeQueryModel> predicate = Optional.ofNullable(searchCompiler.handle(searchComponent))
                .orElse(e -> true);
        return getAllEmployees()
                .filter(predicate);
    }

    @Override
    public Flux<EmployeeQueryModel> paginatedFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {

        Objects.requireNonNull(page, "Page must not be null");
        Objects.requireNonNull(size, "Size must not be null");

        return filterQuery(searchComponent)
                .skip((long) page * size)
                .take(size);

    }

    @Override
    public Mono<Long> countViewEmployees(SearchComponent searchComponent) {
        return filterQuery(searchComponent).count();
    }

    private Mono<EmployeeQueryModel> toQueryModel(Employee employee) {

        Company company = inMemoryStorage.companies.get(employee.getCompanyId().getValue());

        EmployeeQueryModel model = new EmployeeQueryModel();
        model.setId(employee.getId().getValue());
        model.setFirstname(employee.getFullName().getFirstname());
        model.setLastname(employee.getFullName().getLastname());
        model.setEmail(employee.getEmail().getValue());
        model.setCompanyId(employee.getCompanyId().getValue());

        if (company != null) {
            model.setCompanyName(company.getCompanyName().getValue());
            model.setCompanyAddress(company.getAddress().getValue());
        }

        model.setCreatedAt(employee.getCreatedAt());
        model.setUpdatedAt(employee.getUpdatedAt());

        return Mono.just(model);
    }
}