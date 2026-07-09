package com.training.event.inMemory;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import com.training.inMemory.repository.InMemoryEmployeeRepository;
import com.training.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;

public class InMemoryEmployeeCreatedEventListener implements DomainEventListener<EmployeeCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryEmployeeCreatedEventListener.class);
    private final InMemoryEmployeeRepository inMemoryEmployeeRepository;

    public InMemoryEmployeeCreatedEventListener(InMemoryEmployeeRepository inMemoryEmployeeRepository) {
        this.inMemoryEmployeeRepository = inMemoryEmployeeRepository;
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
        return inMemoryEmployeeRepository.createOrUpdateEmployee(null, employee)
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Company with id: {} created", event.getCompanyId()))
                .then();
    }
}
