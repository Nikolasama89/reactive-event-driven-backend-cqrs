package com.training.event.db;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeDeletedEvent;
import com.training.sql.repository.DbEmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class DbEmployeeDeletedEventListener implements DomainEventListener<EmployeeDeletedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DbEmployeeDeletedEventListener.class);
    private final DbEmployeeRepository employeeRepository;

    public DbEmployeeDeletedEventListener(DbEmployeeRepository employeeRepository) {
        this.employeeRepository = employeeRepository;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return employeeRepository.deleteEmployee(event.getEmployeeId().getValue())
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(3)))
                .doOnSuccess(deleted -> logger.info("Employee with id: {} deleted", event.getEmployeeId().getValue()));
    }
}