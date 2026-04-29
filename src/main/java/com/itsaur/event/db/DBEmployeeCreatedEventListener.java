package com.itsaur.event.db;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.model.Employee;
import com.itsaur.repository.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;

public class DBEmployeeCreatedEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DBEmployeeCreatedEventListener.class);
    private final EmployeeRepository employeeRepository;

    public DBEmployeeCreatedEventListener(EmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        Employee employee = Employee.construct(
                event.getEmployeeId().getValue(),
                event.getFullName().getFirstname(),
                event.getFullName().getLastname(),
                event.getEmail().getValue(),
                event.getCompanyId().getValue(),
                OffsetDateTime.now(),
                OffsetDateTime.now()
        );
        return employeeRepository.createOrUpdateEmployee(null, employee)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(3)))
                .doOnSuccess(saved -> logger.info("Employee with id: {} created", event.getEmployeeId().getValue()))
                .then();
    }
}
