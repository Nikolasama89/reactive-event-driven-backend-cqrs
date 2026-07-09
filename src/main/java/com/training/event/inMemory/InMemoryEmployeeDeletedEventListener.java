package com.training.event.inMemory;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeDeletedEvent;
import com.training.inMemory.repository.InMemoryEmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class InMemoryEmployeeDeletedEventListener implements DomainEventListener<EmployeeDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryEmployeeDeletedEventListener.class);
    private final InMemoryEmployeeRepository inMemoryEmployeeRepository;

    public InMemoryEmployeeDeletedEventListener(InMemoryEmployeeRepository inMemoryEmployeeRepository) {
        this.inMemoryEmployeeRepository = inMemoryEmployeeRepository;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return inMemoryEmployeeRepository.deleteEmployee(event.getEmployeeId().getValue())
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Company with id: {} deleted", event.getCompanyId()))
                .then();
    }
}
