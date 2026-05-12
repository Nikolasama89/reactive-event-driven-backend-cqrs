package com.itsaur.event.handler;

import com.itsaur.event.DomainEventListener;
import com.itsaur.event.EmployeeDeletedEvent;
import com.itsaur.event.db.DBEmployeeDeletedEventListener;
import com.itsaur.event.logging.EmployeeDeletedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DBEmployeeDeletedEventHandler implements DomainEventListener<EmployeeDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DBEmployeeDeletedEventHandler.class);
    private final DBEmployeeDeletedEventListener dbEmployeeDeletedEventListener;
    private final EmployeeDeletedLoggingEventListener employeeDeletedLoggingEventListener;

    public DBEmployeeDeletedEventHandler(
            DBEmployeeDeletedEventListener dbEmployeeDeletedEventListener,
            EmployeeDeletedLoggingEventListener employeeDeletedLoggingEventListener
    ) {
        this.dbEmployeeDeletedEventListener = dbEmployeeDeletedEventListener;
        this.employeeDeletedLoggingEventListener = employeeDeletedLoggingEventListener;
    }

    @Override
    public Class<EmployeeDeletedEvent> eventType() {
        return EmployeeDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(EmployeeDeletedEvent event) {
        return dbEmployeeDeletedEventListener.handle(event)
                .then(employeeDeletedLoggingEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Employee with id: {} deleted successfully", event.aggregateId()))
                .doOnError(e -> logger.error("Failed to delete employee with id: {}", event.aggregateId()));
    }
}
