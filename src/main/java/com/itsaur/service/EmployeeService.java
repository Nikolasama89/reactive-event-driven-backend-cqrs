package com.itsaur.service;

import com.itsaur.dto.EmployeeCreateDTO;
import com.itsaur.dto.EmployeeUpdateDTO;
import com.itsaur.model.Employee;
import reactor.core.publisher.Mono;

public interface EmployeeService {

    Mono<Employee> save(EmployeeCreateDTO employeeCreateDTO);
    Mono<Void> update(Long id, EmployeeUpdateDTO employeeUpdateDTO);
    Mono<Void> delete(Long id);
}
