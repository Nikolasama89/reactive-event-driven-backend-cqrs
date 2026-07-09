package com.training.event.inMemory;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeUpdatedEvent;
import com.training.inMemory.repository.InMemoryEmployeeRepository;
import com.training.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;

public class InMemoryEmployeeUpdatedEventListener implements DomainEventListener<EmployeeUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryEmployeeUpdatedEventListener.class);
    private final InMemoryEmployeeRepository inMemoryEmployeeRepository;

    public InMemoryEmployeeUpdatedEventListener(InMemoryEmployeeRepository inMemoryEmployeeRepository) {
        this.inMemoryEmployeeRepository = inMemoryEmployeeRepository;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        Employee employee = Employee.construct(
                event.getEmployeeId().getValue(),
                event.getFullName().getFirstname(),
                event.getFullName().getLastname(),
                event.getEmail().getValue(),
                event.getCompanyId().getValue(),
                event.getOccurredAt(),
                OffsetDateTime.now()
        );
        return inMemoryEmployeeRepository.createOrUpdateEmployee(event.getEmployeeId().getValue(), employee)
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Employee with id: {} updated", event.getCompanyId()))
                .then();
    }
}
