package com.itsaur.event.logging;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.DomainEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class CompanyCreatedLoggingEventListener implements DomainEventListener<CompanyCreatedEvent> {

    private final Logger logger = LoggerFactory.getLogger(CompanyCreatedLoggingEventListener.class);

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        return Mono.fromRunnable(() ->
                logger.info("Company created with id: {}", event.getCompanyId())
        );
    }
}
