package com.itsaur.event.logging;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeUpdatedEvent;
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
