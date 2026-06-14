package com.itsaur.config;

import com.itsaur.event.*;
import com.itsaur.event.db.*;
import com.itsaur.event.logging.*;
import com.itsaur.event.rabbitMQ.RabbitConsumer;
import com.itsaur.event.rabbitMQ.RabbitDomainEventPublisher;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.service.CompanyQueryServiceImpl;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeQueryServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.sql.repository.DbCompanyQueryRepository;
import com.itsaur.sql.repository.DbCompanyRepository;
import com.itsaur.sql.repository.DbEmployeeQueryRepository;
import com.itsaur.sql.repository.DbEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import io.r2dbc.spi.ConnectionFactory;

import java.util.List;

public class SqlRabbitInitializer implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {
        ConnectionFactory connectionFactory = DbConnector.getPool();
        Channel publisherChannel = RabbitMQConfig.getChannel();
        String exchange = "domain.events";

        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        var employeeRepository = new DbEmployeeRepository(connectionFactory);
        var employeeQueryRepository = new DbEmployeeQueryRepository(connectionFactory);
        var companyRepository = new DbCompanyRepository(connectionFactory);
        var companyQueryRepository = new DbCompanyQueryRepository(connectionFactory);

        var domainSerializer = new DomainEventSerializer(serializerMap);
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
        DomainEventListener<CompanyCreatedEvent> dbCompanyCreatedEventListener =
                new DbCompanyCreatedEventListener(companyRepository);
        DomainEventListener<CompanyUpdatedEvent> dbCompanyUpdatedEventListener =
                new DbCompanyUpdatedEventListener(companyRepository);
        DomainEventListener<CompanyDeletedEvent> dbCompanyDeletedEventListener =
                new DbCompanyDeletedEventListener(companyRepository);
        DomainEventListener<EmployeeCreatedEvent> dbEmployeeCreatedEventListener =
                new DbEmployeeCreatedEventListener(employeeRepository);
        DomainEventListener<EmployeeUpdatedEvent> dbEmployeeUpdatedEventListener =
                new DbEmployeeUpdatedEventListener(employeeRepository);
        DomainEventListener<EmployeeDeletedEvent> dbEmployeeDeletedEventListener =
                new DbEmployeeDeletedEventListener(employeeRepository);

        List<DomainEventListener<?>> listeners = List.of(
                dbCompanyCreatedEventListener,
                companyCreatedLoggingEventListener,
                dbCompanyUpdatedEventListener,
                companyUpdatedLoggingListener,
                dbCompanyDeletedEventListener,
                companyDeletedLoggingEventListener,
                dbEmployeeCreatedEventListener,
                employeeCreatedLoggingEventListener,
                dbEmployeeUpdatedEventListener,
                employeeUpdatedLoggingEventListener,
                dbEmployeeDeletedEventListener,
                employeeDeletedLoggingEventListener
        );

        RabbitConsumer companyConsumer = new RabbitConsumer(
                publisherChannel, "company.queue", listeners, deserializerMap, exchange,
                new String[]{"CompanyCreatedEvent", "CompanyUpdatedEvent", "CompanyDeletedEvent"}
        );
        RabbitConsumer employeeConsumer = new RabbitConsumer(publisherChannel, "employee.queue", listeners,
                deserializerMap, exchange,
                new String[]{"EmployeeCreatedEvent", "EmployeeUpdatedEvent", "EmployeeDeletedEvent"});

        companyConsumer.startConsuming();
        employeeConsumer.startConsuming();

        System.out.println("Running with SQL storage and RabbitMQ");

        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
