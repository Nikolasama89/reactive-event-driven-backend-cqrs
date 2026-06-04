//package com.itsaur.event.db;
//
//import com.itsaur.event.CompanyCreatedEvent;
//import com.itsaur.event.DomainEventListener;
//import com.itsaur.model.Company;
//import com.itsaur.repository.CompanyRepository;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.core.publisher.Mono;
//import reactor.util.retry.Retry;
//
//import java.time.Duration;
//
//public class DbCompanyCreatedEventListener implements DomainEventListener<CompanyCreatedEvent> {
//    private final Logger logger = LoggerFactory.getLogger(DbCompanyCreatedEventListener.class);
//    private final CompanyRepository companyRepository;
//
//    public DbCompanyCreatedEventListener(CompanyRepository companyRepository) {
//        this.companyRepository = companyRepository;
//    }
//
//    @Override
//    public Class<CompanyCreatedEvent> eventType() {
//        return CompanyCreatedEvent.class;
//    }
//
//    @Override
//    public Mono<Void> handle(CompanyCreatedEvent event) {
//        Company company = Company.construct(
//                event.getCompanyId().getValue(),
//                event.getCompanyName().getValue(),
//                event.getVat().getValue(),
//                event.getAddress().getValue(),
//                event.getOccurredAt(),
//                event.getOccurredAt()
//        );
//        return companyRepository.createOrUpdateCompany(null, company)
//                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
//                        .maxBackoff(Duration.ofSeconds(3)))
//                .doOnSuccess(saved -> logger.info("Company with id: {} created", event.getCompanyId()))
//                .then();
//    }
//}