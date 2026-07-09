package com.training.inMemory.repository;

import com.training.model.Employee;
import com.training.repository.EmployeeRepository;
import com.training.inMemory.search.InMemoryStorage;
import reactor.core.publisher.Mono;


public class InMemoryEmployeeRepository implements EmployeeRepository {

    private final InMemoryStorage inMemoryStorage;

    public InMemoryEmployeeRepository(InMemoryStorage inMemoryStorage){
        this.inMemoryStorage = inMemoryStorage;
    }

    @Override
    public Mono<Employee> getEmployeeById(Long id) {
        return Mono.justOrEmpty(inMemoryStorage.getEmployees().get(id));
    }

    public Mono<Employee> createOrUpdateEmployee(Long id, Employee employee) {
        if (id == null){
            long newId = inMemoryStorage.getIdGenerator().incrementAndGet();
            inMemoryStorage.getEmployees().put(newId, employee);
            return Mono.just(employee);
        }
        return Mono.justOrEmpty(inMemoryStorage.getEmployees().get(id))
                .map(existing -> {

                    Employee updated = existing.update(
                            employee.getFullName().getFirstname(),
                            employee.getFullName().getLastname());
                    inMemoryStorage.getEmployees().put(id, updated);
                    return updated;
                });
    }

    public Mono<Void> deleteEmployee(Long id) {
        inMemoryStorage.getEmployees().remove(id);
        return Mono.empty();
    }

    @Override
    public Mono<Boolean> existsEmail(String email) {
        boolean exists = inMemoryStorage.getEmployees().values().stream().anyMatch(employee ->
                employee.getEmail().getValue() != null && employee.getEmail().getValue().equalsIgnoreCase(email));
        return Mono.just(exists);
    }
}
