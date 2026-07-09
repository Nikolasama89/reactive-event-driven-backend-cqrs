package com.training.service;

import com.training.dto.EmployeeCreateDTO;
import com.training.dto.EmployeeUpdateDTO;
import com.training.model.Employee;
import reactor.core.publisher.Mono;

public interface EmployeeService {

    Mono<Employee> save(EmployeeCreateDTO employeeCreateDTO);
    Mono<Void> update(Long id, EmployeeUpdateDTO employeeUpdateDTO);
    Mono<Void> delete(Long id);
}
