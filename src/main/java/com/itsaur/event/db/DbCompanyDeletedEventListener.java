package com.itsaur.event.db;

import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.sql.repository.DbCompanyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

public class DbCompanyDeletedEventListener implements DomainEventListener<CompanyDeletedEvent> {

    private final Logger logger = LoggerFactory.getLogger(DbCompanyDeletedEventListener.class);
    private final DbCompanyRepository companyRepository;

    public DbCompanyDeletedEventListener(DbCompanyRepository companyRepository) {
        this.companyRepository = companyRepository;
    }

    @Override
    public Class<CompanyDeletedEvent> eventType() {
        return CompanyDeletedEvent.class;
    }

    @Override
    public Mono<Void> handle(CompanyDeletedEvent event) {
        return companyRepository.deleteCompany(event.getCompanyId().getValue())
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(3)))
                .doOnSuccess(deleted -> logger.info("Employee with id: {} deleted", event.getCompanyId()));
    }
}
