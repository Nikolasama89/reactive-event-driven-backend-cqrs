package com.training.event.logging;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class EmployeeUpdatedLoggingEventListener implements DomainEventListener<EmployeeUpdatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(EmployeeUpdatedLoggingEventListener.class);

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        return Mono.fromRunnable(() -> logger.info("Employee with id: {} updated", event.getEmployeeId()));
    }
}
