package com.itsaur.event.inMemory;

import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.model.Company;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class InMemoryCompanyUpdatedEventListener implements DomainEventListener<CompanyUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryCompanyUpdatedEventListener.class);
    private final InMemoryCompanyRepository inMemoryCompanyRepository;

    public InMemoryCompanyUpdatedEventListener(InMemoryCompanyRepository inMemoryCompanyRepository) {
        this.inMemoryCompanyRepository = inMemoryCompanyRepository;
    }

    @Override
    public Class<CompanyUpdatedEvent> eventType() {
        return CompanyUpdatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyUpdatedEvent event) {
        Company company = Company.construct(
                event.getCompanyId().getValue(),
                event.getCompanyName().getValue(),
                event.getVat().getValue(),
                event.getAddress().getValue(),
                event.getOccurredAt(),
                event.getOccurredAt()
        );
        return inMemoryCompanyRepository.createOrUpdateCompany(event.getCompanyId().getValue(), company)
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Company with id: {} updated", event.getCompanyId()))
                .then();
    }
}
