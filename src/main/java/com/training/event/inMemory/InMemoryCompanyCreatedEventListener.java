package com.training.event.inMemory;

import com.training.event.CompanyCreatedEvent;
import com.training.event.DomainEventListener;
import com.training.inMemory.repository.InMemoryCompanyRepository;
import com.training.model.Company;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class InMemoryCompanyCreatedEventListener implements DomainEventListener<CompanyCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(InMemoryCompanyCreatedEventListener.class);
    private final InMemoryCompanyRepository inMemoryCompanyRepository;

    public InMemoryCompanyCreatedEventListener(InMemoryCompanyRepository inMemoryCompanyRepository) {
        this.inMemoryCompanyRepository = inMemoryCompanyRepository;
    }

    @Override
    public Class<CompanyCreatedEvent> eventType() {
        return CompanyCreatedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyCreatedEvent event) {
        Company company = Company.construct(
                event.getCompanyId().getValue(),
                event.getCompanyName().getValue(),
                event.getVat().getValue(),
                event.getAddress().getValue(),
                event.getOccurredAt(),
                event.getOccurredAt()
        );
        return inMemoryCompanyRepository.createOrUpdateCompany(null, company)
                .retryWhen(Retry.backoff(10, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(1)))
                .doOnSuccess(saved -> logger.info("Company with id: {} created", event.getCompanyId()))
                .then();
    }
}