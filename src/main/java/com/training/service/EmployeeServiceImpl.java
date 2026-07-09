package com.training.service;

import com.training.dto.EmployeeCreateDTO;
import com.training.dto.EmployeeUpdateDTO;
import com.training.event.*;
import com.training.exceptions.*;
import com.training.model.Employee;
import com.training.repository.CompanyRepository;
import com.training.repository.EmployeeRepository;
import reactor.core.publisher.Mono;


public class EmployeeServiceImpl implements EmployeeService {

    private final EmployeeRepository employeeRepository;
    private final CompanyRepository companyRepository;
    private final EventPublisher eventPublisher;

    public EmployeeServiceImpl(EmployeeRepository employeeRepository,  CompanyRepository companyRepository, EventPublisher eventPublisher) {
        this.employeeRepository = employeeRepository;
        this.companyRepository = companyRepository;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public Mono<Employee> save(EmployeeCreateDTO employeeCreateDTO) {

        Long companyId = employeeCreateDTO.getCompanyId();

        if (employeeCreateDTO.getCompanyId() == null || companyId <= 0) {
            return Mono.error(new CompanyIdInvalidException("Company id can not be null or empty"));
        }
        if (employeeCreateDTO.getEmail() == null){
            return Mono.error(new EmailRequiredException("Email cannot be null"));
        }
        return employeeRepository.existsEmail(employeeCreateDTO.getEmail()).flatMap(exists -> {
            if (exists) {
                return Mono.error(new EmailAlreadyExistsException("Email already exists"));
            }
            return companyRepository.getCompanyById(companyId)
                    .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + companyId + " not found")))
                    .flatMap(company -> {

                        Employee employee = Employee.create(
                                employeeCreateDTO.getFirstname(),
                                employeeCreateDTO.getLastname(),
                                employeeCreateDTO.getEmail(),
                                employeeCreateDTO.getCompanyId()
                        );

                        EmployeeCreatedEvent event = new EmployeeCreatedEvent(
                                employee.getId(),
                                employee.getCompanyId(),
                                employee.getFullName(),
                                employee.getEmail(),
                                company.getCompanyName(),
                                company.getAddress(),
                                employee.getCreatedAt()
                        );
                        return eventPublisher.publish(event).thenReturn(employee);
                    });
        });
    }

    @Override
    public Mono<Void> update(Long id, EmployeeUpdateDTO employeeUpdateDTO) {

        if (id == null || id <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Id must be greater than zero"));
        }
        return employeeRepository.getEmployeeById(id)
                .switchIfEmpty(Mono.error(new EmployeeNotFoundException("Employee with id: " + id + " not found.")))
                .flatMap(existing -> companyRepository.getCompanyById(existing.getCompanyId().getValue())
                        .flatMap(company -> {

                            Employee updated = existing.update(
                                    employeeUpdateDTO.getFirstname(),
                                    employeeUpdateDTO.getLastname()
                            );

                            EmployeeUpdatedEvent event = new EmployeeUpdatedEvent(
                                    updated.getId(),
                                    updated.getCompanyId(),
                                    updated.getFullName(),
                                    updated.getEmail(),
                                    company.getCompanyName(),
                                    company.getAddress(),
                                    updated.getUpdatedAt()
                            );
                            return eventPublisher.publish(event);
                        })
                );
    }

    @Override
    public Mono<Void> delete(Long id) {
        if (id == null || id <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Id must be greater than zero"));
        }
        return employeeRepository.getEmployeeById(id)
                .switchIfEmpty(Mono.error(new EmployeeNotFoundException("Employee with id: " + id + " not found.")))
                .flatMap(employee -> eventPublisher
                        .publish(new EmployeeDeletedEvent(employee.getId(), employee.getCompanyId())))
                .then();
    }
}