package com.itsaur.repository;

import com.itsaur.model.Employee;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class InMemoryEmployeeRepository implements EmployeeRepository{
    private final InMemoryStorage inMemoryStorage;

    public InMemoryEmployeeRepository(InMemoryStorage inMemoryStorage){
        this.inMemoryStorage = inMemoryStorage;
    }

    @Override
    public Mono<Employee> getEmployeeById(Long id) {
        return Mono.justOrEmpty(inMemoryStorage.getEmployees().get(id));
    }

    @Override
    public Mono<Employee> createOrUpdateEmployee(Long id, Employee employee) {
        if (id == null){
            long newId = inMemoryStorage.getIdGenerator().incrementAndGet();
            employee.setId(newId);
            employee.setCreatedAt(OffsetDateTime.now());
            employee.setUpdatedAt(OffsetDateTime.now());
            inMemoryStorage.getEmployees().put(newId, employee);
            return Mono.just(employee);
        }
        return Mono.justOrEmpty(inMemoryStorage.getEmployees().get(id))
                .map(existing -> {
                    Employee updated = new Employee();
                    updated.setId(existing.getId());
                    updated.setFirstname(employee.getFirstname());
                    updated.setLastname(employee.getLastname());
                    updated.setEmail(existing.getEmail());
                    updated.setCompanyId(existing.getCompanyId());
                    updated.setCreatedAt(existing.getCreatedAt());
                    updated.setUpdatedAt(OffsetDateTime.now());

                    inMemoryStorage.getEmployees().put(id, updated);
                    return updated;
                });
    }

    @Override
    public Mono<Void> deleteEmployee(Long id) {
        inMemoryStorage.getEmployees().remove(id);
        return Mono.empty();
    }

    @Override
    public Mono<Boolean> existsEmail(String email) {
        boolean exists = inMemoryStorage.getEmployees().values().stream().anyMatch(employee ->
                employee.getEmail() != null && employee.getEmail().equalsIgnoreCase(email));
        return Mono.just(exists);
    }
}
