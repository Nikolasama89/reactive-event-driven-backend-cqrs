package com.itsaur.event.db;

import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.model.Company;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.sql.repository.DbCompanyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.OffsetDateTime;

public class DbCompanyUpdatedEventListener implements DomainEventListener<CompanyUpdatedEvent> {
    private final Logger logger = LoggerFactory.getLogger(DbCompanyUpdatedEventListener.class);
    private final DbCompanyRepository companyRepository;

    public DbCompanyUpdatedEventListener(DbCompanyRepository companyRepository) {
        this.companyRepository = companyRepository;
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
                OffsetDateTime.now()
        );
        return companyRepository.createOrUpdateCompany(event.getCompanyId().getValue(), company)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(3)))
                .doOnSuccess(saved -> logger.info("Company with id: {} updated", event.getCompanyId()))
                .then();
    }
}
