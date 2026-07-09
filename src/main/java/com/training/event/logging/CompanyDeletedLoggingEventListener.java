package com.training.event.logging;

import com.training.event.CompanyDeletedEvent;
import com.training.event.DomainEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class CompanyDeletedLoggingEventListener implements DomainEventListener<CompanyDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(CompanyDeletedLoggingEventListener.class);

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return Mono.fromRunnable(() ->
                logger.info("Company with id: {} deleted", event.getCompanyId()));
    }
}
