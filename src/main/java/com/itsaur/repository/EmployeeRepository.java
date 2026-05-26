package com.itsaur.repository;

import com.itsaur.model.Employee;
import reactor.core.publisher.Mono;

public interface EmployeeRepository {
    Mono<Employee> getEmployeeById(Long id);
    Mono<Boolean> existsEmail(String email);
}
