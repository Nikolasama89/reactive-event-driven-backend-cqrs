package com.itsaur.service;

import com.itsaur.dto.EmployeeCreateDTO;
import com.itsaur.dto.EmployeeUpdateDTO;
import com.itsaur.exceptions.*;
import com.itsaur.mapper.EmployeeMapper;
import com.itsaur.model.Employee;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


public class EmployeeServiceImpl implements EmployeeService {

    private static final Logger log = LoggerFactory.getLogger(EmployeeServiceImpl.class);
    private final EmployeeRepository employeeRepository;
    private final CompanyRepository companyRepository;
    private final EmployeeMapper employeeMapper =  new EmployeeMapper();
    
    public EmployeeServiceImpl(EmployeeRepository employeeRepository,  CompanyRepository companyRepository) {
        this.employeeRepository = employeeRepository;
        this.companyRepository = companyRepository;
    }

    @Override
    public Mono<Employee> save(EmployeeCreateDTO employeeCreateDTO) {

        Long companyId = employeeCreateDTO.getCompanyId();

        if (employeeCreateDTO.getCompanyId() == null || companyId <= 0) {
            return Mono.error(new CompanyIdInvalidException("Company id can not be null or empty"));
        }

        if (employeeCreateDTO.getFirstname() == null || employeeCreateDTO.getFirstname().isEmpty()) {
            return Mono.error(new FirstnameRequiredException("Firstname is required"));
        } else if (employeeCreateDTO.getFirstname().length() > 50 || employeeCreateDTO.getFirstname().length() < 3) {
            return Mono.error(new FirstnameInvalidCharacterLengthException("Firstname must be between 3 and 50 characters"));
        }

        if (employeeCreateDTO.getEmail() == null || employeeCreateDTO.getEmail().isEmpty()) {
            return Mono.error(new EmailRequiredException("Email is required"));
        } else if (employeeCreateDTO.getEmail().length() > 50 || employeeCreateDTO.getEmail().length() < 3) {
            return Mono.error(new EmailInvalidCharacterLengthException("Email must be between 3 and 50 characters"));
        }

        if (employeeCreateDTO.getLastname() == null || employeeCreateDTO.getLastname().isEmpty()) {
            return Mono.error(new LastnameRequiredException("Lastname is required"));
        } else if (employeeCreateDTO.getLastname().length() > 50 || employeeCreateDTO.getLastname().length() < 3) {
            return Mono.error(new LastnameInvalidCharacterLengthException("Lastname must be between 3 and 50 characters"));
        }

        return employeeRepository.existsEmail(employeeCreateDTO.getEmail()).flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new EmailAlreadyExistsException("Email already exists"));
                    }
                    return companyRepository.getCompanyById(companyId)
                            .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + companyId + " not found")))
                            .flatMap(company -> {
                                Employee employee = employeeMapper.toEntity(employeeCreateDTO);
                                // Αν βρεθει η εταρεια με το id που δωσαμε τοτε θα προχωρησει στο save
                                return employeeRepository.createOrUpdateEmployee(null, employee);
                            });
                });
    }

    @Override
    public Mono<Void> update(Long id, EmployeeUpdateDTO employeeUpdateDTO) {

        if (employeeUpdateDTO.getFirstname() == null || employeeUpdateDTO.getFirstname().isEmpty()) {
            return Mono.error(new FirstnameRequiredException("Firstname is required"));
        } else if (employeeUpdateDTO.getFirstname().length() > 50 || employeeUpdateDTO.getFirstname().length() < 3) {
            return Mono.error(new FirstnameInvalidCharacterLengthException("Firstname must be between 3 and 50 characters"));
        }

        if (employeeUpdateDTO.getLastname() == null || employeeUpdateDTO.getLastname().isEmpty()) {
            return Mono.error(new LastnameRequiredException("Lastname is required"));
        } else if (employeeUpdateDTO.getLastname().length() > 50 || employeeUpdateDTO.getLastname().length() < 3) {
            return Mono.error(new LastnameInvalidCharacterLengthException("Lastname must be between 3 and 50 characters"));
        }

        return employeeRepository.getEmployeeById(id)
                .switchIfEmpty(Mono.error(new EmployeeNotFoundException("Employee with id: " + id + " not found.")))
                .flatMap(employee -> {
                    employee.setFirstname(employeeUpdateDTO.getFirstname());
                    employee.setLastname(employeeUpdateDTO.getLastname());
                    return employeeRepository.createOrUpdateEmployee(id, employee);
                })
                .then();
    }

    @Override
    public Mono<Void> delete(Long id) {
        if (id == null || id <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Id must be greater than zero"));
        }
        return employeeRepository.getEmployeeById(id)
                .switchIfEmpty(Mono.error(new EmployeeNotFoundException("Employee with id: " + id + " not found.")))
                .flatMap(employee -> employeeRepository.deleteEmployee(id));
    }
}
