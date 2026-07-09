package com.training.event.db;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import com.training.model.Employee;
import com.training.sql.repository.DbEmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;

public class DbEmployeeCreatedEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DbEmployeeCreatedEventListener.class);
    private final DbEmployeeRepository employeeRepository;

    public DbEmployeeCreatedEventListener(DbEmployeeRepository employeeRepository) {
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
