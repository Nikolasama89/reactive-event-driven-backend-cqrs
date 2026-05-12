package com.itsaur.event.handler;

import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.db.DBCompanyDeletedEventListener;
import com.itsaur.event.logging.CompanyDeletedLoggingEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class DBCompanyDeletedEventHandler implements DomainEventListener<CompanyDeletedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DBCompanyDeletedEventHandler.class);
    private final DBCompanyDeletedEventListener dbCompanyDeletedEventListener;
    private final CompanyDeletedLoggingEventListener companyDeletedLoggingEventListener;

    public DBCompanyDeletedEventHandler(
            DBCompanyDeletedEventListener dbCompanyDeletedEventListener,
            CompanyDeletedLoggingEventListener companyDeletedLoggingEventListener
    ) {
        this.dbCompanyDeletedEventListener = dbCompanyDeletedEventListener;
        this.companyDeletedLoggingEventListener = companyDeletedLoggingEventListener;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return dbCompanyDeletedEventListener.handle(event)
                .then(companyDeletedLoggingEventListener.handle(event))
                .doOnSuccess(s -> logger.info("Company with id: {} deleted successfully", event.aggregateId()))
                .doOnError(e -> logger.error("Failed to delete company with id: {}", event.aggregateId()));
    }
}
