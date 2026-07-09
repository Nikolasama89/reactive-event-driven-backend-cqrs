package com.training.event.db;

import com.training.event.CompanyCreatedEvent;
import com.training.event.DomainEventListener;
import com.training.model.Company;
import com.training.sql.repository.DbCompanyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class DbCompanyCreatedEventListener implements DomainEventListener<CompanyCreatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DbCompanyCreatedEventListener.class);
    private final DbCompanyRepository companyRepository;

    public DbCompanyCreatedEventListener(DbCompanyRepository companyRepository) {
        this.companyRepository = companyRepository;
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
        return companyRepository.createOrUpdateCompany(null, company)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(3)))
                .doOnSuccess(saved -> logger.info("Company with id: {} created", event.getCompanyId()))
                .then();
    }
}