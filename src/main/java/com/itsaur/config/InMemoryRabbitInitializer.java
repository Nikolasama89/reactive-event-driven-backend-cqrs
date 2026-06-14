package com.itsaur.config;

import com.itsaur.event.DomainEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.inMemory.*;
import com.itsaur.event.logging.*;
import com.itsaur.event.rabbitMQ.RabbitConsumer;
import com.itsaur.event.rabbitMQ.RabbitDomainEventPublisher;
import com.itsaur.inMemory.repository.InMemoryCompanyQueryRepository;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeQueryRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeRepository;
import com.itsaur.inMemory.search.InMemoryCompanySearch;
import com.itsaur.inMemory.search.InMemoryEmployeeSearch;
import com.itsaur.inMemory.search.InMemoryStorage;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.serializer.Serializer;
import com.itsaur.service.CompanyQueryServiceImpl;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeQueryServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.util.RabbitMQConfig;
import com.rabbitmq.client.Channel;

import java.util.List;

public class InMemoryRabbitInitializer implements AppEnvInitializer{

    @Override
    public AppContext initialize() throws Exception {
        Channel publisherChannel = RabbitMQConfig.getChannel();
        String exchange = "domain.events";
        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        InMemoryStorage storage = new InMemoryStorage();
        InMemoryEmployeeSearch employeeSearch = new InMemoryEmployeeSearch(storage);
        InMemoryCompanySearch companySearch = new InMemoryCompanySearch(storage);

        Serializer<DomainEvent> domainSerializer = new DomainEventSerializer(serializerMap);

        var employeeRepository = new InMemoryEmployeeRepository(storage);
        var employeeQueryRepository = new InMemoryEmployeeQueryRepository(storage, employeeSearch);
        var companyRepository = new InMemoryCompanyRepository(storage);
        var companyQueryRepository = new InMemoryCompanyQueryRepository(storage, companySearch);
        var eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);

        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
        var companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

        DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
        DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
        DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
        DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
        DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
        DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();
        DomainEventListener<?> inMemoryCompanyCreatedEventListener = new InMemoryCompanyCreatedEventListener(companyRepository);
        DomainEventListener<?> inMemoryCompanyUpdatedEventListener = new InMemoryCompanyUpdatedEventListener(companyRepository);
        DomainEventListener<?> inMemoryCompanyDeletedEventListener = new InMemoryCompanyDeletedEventListener(companyRepository);
        DomainEventListener<?> inMemoryEmployeeCreatedEventListener = new InMemoryEmployeeCreatedEventListener(employeeRepository);
        DomainEventListener<?> inMemoryEmployeeUpdatedEventListener = new InMemoryEmployeeUpdatedEventListener(employeeRepository);
        DomainEventListener<?> inMemoryEmployeeDeletedEventListener = new InMemoryEmployeeDeletedEventListener(employeeRepository);

        List<DomainEventListener<?>> listeners = List.of(
                inMemoryCompanyCreatedEventListener,
                companyCreatedLoggingEventListener,
                inMemoryCompanyUpdatedEventListener,
                companyUpdatedLoggingListener,
                inMemoryCompanyDeletedEventListener,
                companyDeletedLoggingEventListener,
                inMemoryEmployeeCreatedEventListener,
                employeeCreatedLoggingEventListener,
                inMemoryEmployeeUpdatedEventListener,
                employeeUpdatedLoggingEventListener,
                inMemoryEmployeeDeletedEventListener,
                employeeDeletedLoggingEventListener
        );

        RabbitConsumer companyConsumer = new RabbitConsumer(
                publisherChannel,
                "company.queue",
                listeners,
                deserializerMap,
                exchange,
                new String[]{"CompanyCreatedEvent", "CompanyUpdatedEvent", "CompanyDeletedEvent"}
        );

        RabbitConsumer employeeConsumer = new RabbitConsumer(
                publisherChannel,
                "employee.queue",
                listeners,
                deserializerMap,
                exchange,
                new String[]{"EmployeeCreatedEvent", "EmployeeUpdatedEvent", "EmployeeDeletedEvent"});

        companyConsumer.startConsuming();
        employeeConsumer.startConsuming();

        System.out.println("Running IN-MEMORY with RABBITMQ");

        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
