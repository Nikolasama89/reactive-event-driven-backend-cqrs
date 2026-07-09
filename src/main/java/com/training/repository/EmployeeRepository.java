package com.training.repository;

import com.training.model.Employee;
import reactor.core.publisher.Mono;

public interface EmployeeRepository {
    Mono<Employee> getEmployeeById(Long id);
    Mono<Boolean> existsEmail(String email);
}
