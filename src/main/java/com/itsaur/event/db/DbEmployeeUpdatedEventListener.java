//package com.itsaur.event.db;
//
//import com.itsaur.event.DomainEventListener;
//import com.itsaur.event.EmployeeUpdatedEvent;
//import com.itsaur.model.Employee;
//import com.itsaur.repository.EmployeeRepository;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.core.publisher.Mono;
//import reactor.util.retry.Retry;
//
//import java.time.Duration;
//import java.time.OffsetDateTime;
//
//public class DbEmployeeUpdatedEventListener implements DomainEventListener<EmployeeUpdatedEvent> {
//
//    private final Logger logger = LoggerFactory.getLogger(DbEmployeeUpdatedEventListener.class);
//    private final EmployeeRepository employeeRepository;
//
//    public DbEmployeeUpdatedEventListener(EmployeeRepository employeeRepository) {
//        this.employeeRepository = employeeRepository;
//    }
//
//    @Override
//    public Class<EmployeeUpdatedEvent> eventType() {
//        return EmployeeUpdatedEvent.class;
//    }
//
//    @Override
//    public Mono<Void> handle(EmployeeUpdatedEvent event) {
//        Employee employee = Employee.construct(
//                event.getEmployeeId().getValue(),
//                event.getFullName().getFirstname(),
//                event.getFullName().getLastname(),
//                event.getEmail().getValue(),
//                event.getCompanyId().getValue(),
//                event.getOccurredAt(),
//                OffsetDateTime.now()
//        );
//        return employeeRepository.createOrUpdateEmployee(event.getEmployeeId().getValue(), employee)
//                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
//                        .maxBackoff(Duration.ofSeconds(3)))
//                .doOnSuccess(saved -> logger.info("Employee with id: {} updated", event.getEmployeeId().getValue()))
//                .then();
//    }
//}