package com.itsaur.event.handler;

import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.db.DBCompanyUpdatedEventListener;
import com.itsaur.event.logging.CompanyUpdatedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DBCompanyUpdatedEventHandler implements DomainEventListener<CompanyUpdatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DBCompanyUpdatedEventHandler.class);
    private final DBCompanyUpdatedEventListener dbCompanyUpdatedEventListener;
    private final CompanyUpdatedLoggingEventListener companyUpdatedLoggingEventListener;

    public DBCompanyUpdatedEventHandler(
            DBCompanyUpdatedEventListener dbCompanyUpdatedEventListener,
            CompanyUpdatedLoggingEventListener companyUpdatedLoggingEventListener
    ) {
        this.dbCompanyUpdatedEventListener = dbCompanyUpdatedEventListener;
        this.companyUpdatedLoggingEventListener = companyUpdatedLoggingEventListener;
    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return dbCompanyUpdatedEventListener.handle(event)
                .then(companyUpdatedLoggingEventListener.handle(event)).doOnSuccess(s ->
                        logger.info("Company with id {} updated successfully", event.aggregateId()))
                .doOnError(e -> logger.info("Failed to update company with id: {}", event.aggregateId()));
    }
}
