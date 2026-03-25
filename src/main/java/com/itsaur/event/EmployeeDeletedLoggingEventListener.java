package com.itsaur.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class EmployeeDeletedLoggingEventListener implements DomainEventListener<EmployeeDeletedEvent>{

    private final Logger logger = LoggerFactory.getLogger(EmployeeDeletedLoggingEventListener.class);

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return Mono.fromRunnable(() ->
                logger.info("Employee with id: {} and companyId: {} deleted", event.getEmployeeId(), event.getCompanyId()));
    }
}
