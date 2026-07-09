package com.training.config;

import com.training.event.DomainEvent;
import com.training.event.DomainEventListener;
import com.training.event.inMemory.*;
import com.training.event.logging.*;
import com.training.event.rabbitMQ.RabbitConsumer;
import com.training.event.rabbitMQ.RabbitDomainEventPublisher;
import com.training.inMemory.repository.InMemoryCompanyQueryRepository;
import com.training.inMemory.repository.InMemoryCompanyRepository;
import com.training.inMemory.repository.InMemoryEmployeeQueryRepository;
import com.training.inMemory.repository.InMemoryEmployeeRepository;
import com.training.inMemory.search.InMemoryCompanySearch;
import com.training.inMemory.search.InMemoryEmployeeSearch;
import com.training.inMemory.search.InMemoryStorage;
import com.training.serializer.DomainEventSerializer;
import com.training.serializer.Serializer;
import com.training.service.CompanyQueryServiceImpl;
import com.training.service.CompanyServiceImpl;
import com.training.service.EmployeeQueryServiceImpl;
import com.training.service.EmployeeServiceImpl;
import com.training.util.RabbitMQConfig;
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
