package com.training.event.logging;

import com.training.event.DomainEventListener;
import com.training.event.EmployeeCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class EmployeeCreatedLoggingEventListener implements DomainEventListener<EmployeeCreatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(EmployeeCreatedLoggingEventListener.class);

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        return Mono.fromRunnable(() -> logger.info("Employee with id: {} and companyId: {} created.", event.getEmployeeId(), event.getCompanyId()));
    }
}
