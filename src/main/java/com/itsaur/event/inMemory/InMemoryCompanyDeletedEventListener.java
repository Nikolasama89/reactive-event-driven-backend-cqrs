package com.itsaur.event.inMemory;

import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class InMemoryCompanyDeletedEventListener implements DomainEventListener<CompanyDeletedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryCompanyDeletedEventListener.class);
    private final InMemoryCompanyRepository inMemoryCompanyRepository;

    public InMemoryCompanyDeletedEventListener(InMemoryCompanyRepository inMemoryCompanyRepository) {
        this.inMemoryCompanyRepository = inMemoryCompanyRepository;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return inMemoryCompanyRepository.deleteCompany(event.getCompanyId().getValue())
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Company with id: {} deleted", event.getCompanyId()))
                .then();
    }
}
