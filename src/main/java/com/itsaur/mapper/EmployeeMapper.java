package com.itsaur.mapper;

import com.itsaur.dto.EmployeeCreateDTO;
import com.itsaur.model.Employee;

public class EmployeeMapper {

    public Employee toEntity(EmployeeCreateDTO employeeCreateDTO) {
        Employee employee = new Employee();
        employee.setFirstname(employeeCreateDTO.getFirstname());
        employee.setLastname(employeeCreateDTO.getLastname());
        employee.setEmail(employeeCreateDTO.getEmail());
        employee.setCompanyId(employeeCreateDTO.getCompanyId());
        return employee;
    }
}
