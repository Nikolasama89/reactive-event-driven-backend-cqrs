package com.itsaur.event.handler;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.db.DBCompanyCreatedEventListener;
import com.itsaur.event.logging.CompanyCreatedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


public class DBCompanyCreatedEventHandler implements DomainEventListener<CompanyCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DBCompanyCreatedEventHandler.class);
    private final DBCompanyCreatedEventListener dbCompanyCreatedEventListener;
    private final CompanyCreatedLoggingEventListener createdLoggingEventListener;

    public DBCompanyCreatedEventHandler(
            DBCompanyCreatedEventListener dbCompanyCreatedEventListener,
            CompanyCreatedLoggingEventListener createdLoggingEventListener
    ) {
        this.dbCompanyCreatedEventListener = dbCompanyCreatedEventListener;
        this.createdLoggingEventListener = createdLoggingEventListener;
    }

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        return dbCompanyCreatedEventListener.handle(event)
                .then(createdLoggingEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Company created successfully with id {}", event.aggregateId()))
                .doOnError(e -> logger.error("Failed to create company with id {} : {}", event.aggregateId(), e.getMessage()));
    }
}
