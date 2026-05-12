package com.itsaur.event.handler;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeUpdatedEvent;
import com.itsaur.event.db.DBEmployeeUpdatedEventListener;
import com.itsaur.event.logging.EmployeeUpdatedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DBEmployeeUpdatedEventHandler implements DomainEventListener<EmployeeUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DBEmployeeUpdatedEventHandler.class);
    private final DBEmployeeUpdatedEventListener dbEmployeeUpdatedEventListener;
    private final EmployeeUpdatedLoggingEventListener employeeUpdatedLoggingEventListener;

    public DBEmployeeUpdatedEventHandler(
            DBEmployeeUpdatedEventListener dbEmployeeUpdatedEventListener,
            EmployeeUpdatedLoggingEventListener employeeUpdatedLoggingEventListener
    ) {
        this.dbEmployeeUpdatedEventListener = dbEmployeeUpdatedEventListener;
        this.employeeUpdatedLoggingEventListener = employeeUpdatedLoggingEventListener;
    }

    @Override
    public Class<EmployeeUpdatedEvent> eventType() {
        return EmployeeUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeUpdatedEvent event) {
        return dbEmployeeUpdatedEventListener.handle(event)
                .then(employeeUpdatedLoggingEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee with id {} updated successfully", event.aggregateId()))
                .doOnError(e -> logger.error("Failed to update employee with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}
