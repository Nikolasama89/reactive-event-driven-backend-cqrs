package com.itsaur.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class CompanyUpdatedLoggingEventListener implements DomainEventListener<CompanyUpdatedEvent>{
    private final Logger logger = LoggerFactory.getLogger(CompanyUpdatedLoggingEventListener.class);

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        return Mono.fromRunnable(() -> logger.info("Company with id: {} updated", event.getCompanyId()));
    }
}
