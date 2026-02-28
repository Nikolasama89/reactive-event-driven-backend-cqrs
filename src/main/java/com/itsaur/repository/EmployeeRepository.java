package com.itsaur.repository;

import com.itsaur.model.Employee;
import reactor.core.publisher.Mono;

public interface EmployeeRepository {
    Mono<Employee> getEmployeeById(Long id);
    Mono<Employee> createOrUpdateEmployee(Long id, Employee employee);
    Mono<Void> deleteEmployee(Long id);
    Mono<Boolean> existsEmail(String email);
}
