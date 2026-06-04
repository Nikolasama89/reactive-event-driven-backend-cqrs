//package com.itsaur.event.db;
//
//import com.itsaur.event.DomainEventListener;
//import com.itsaur.event.EmployeeDeletedEvent;
//import com.itsaur.repository.EmployeeRepository;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import reactor.core.publisher.Mono;
//import reactor.util.retry.Retry;
//
//import java.time.Duration;
//
//public class DbEmployeeDeletedEventListener implements DomainEventListener<EmployeeDeletedEvent> {
//
//    private final Logger logger = LoggerFactory.getLogger(DbEmployeeDeletedEventListener.class);
//    private final EmployeeRepository employeeRepository;
//
//    public DbEmployeeDeletedEventListener(EmployeeRepository employeeRepository) {
//        this.employeeRepository = employeeRepository;
//    }
//
//    @Override
//    public Class<EmployeeDeletedEvent> eventType() {
//        return EmployeeDeletedEvent.class;
//    }
//
//    @Override
//    public Mono<Void> handle(EmployeeDeletedEvent event) {
//        return employeeRepository.deleteEmployee(event.getEmployeeId().getValue())
//                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
//                        .maxBackoff(Duration.ofSeconds(3)))
//                .doOnSuccess(deleted -> logger.info("Employee with id: {} deleted", event.getEmployeeId().getValue()));
//    }
//}