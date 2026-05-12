package com.itsaur.event.handler;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.event.db.DBEmployeeCreatedEventListener;
import com.itsaur.event.logging.EmployeeCreatedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DBEmployeeCreatedEventHandler implements DomainEventListener<EmployeeCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DBEmployeeCreatedEventHandler.class);
    private final DBEmployeeCreatedEventListener dbEmployeeCreatedEventListener;
    private final EmployeeCreatedLoggingEventListener employeeCreatedLoggingEventListener;

    public DBEmployeeCreatedEventHandler(
            DBEmployeeCreatedEventListener dbEmployeeCreatedEventListener,
            EmployeeCreatedLoggingEventListener employeeCreatedLoggingEventListener
    ) {
        this.dbEmployeeCreatedEventListener = dbEmployeeCreatedEventListener;
        this.employeeCreatedLoggingEventListener = employeeCreatedLoggingEventListener;
    }

    @Override
    public Class<EmployeeCreatedEvent> eventType() {
        return EmployeeCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeCreatedEvent event) {
        return dbEmployeeCreatedEventListener.handle(event)
                .then(employeeCreatedLoggingEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee created successfully with id {}", event.aggregateId()))
                .doOnError(e -> logger.error("Failed to create employee with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}