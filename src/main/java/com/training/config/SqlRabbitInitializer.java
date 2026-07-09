package com.training.config;

import com.training.event.*;
import com.training.event.db.*;
import com.training.event.logging.*;
import com.training.event.rabbitMQ.RabbitConsumer;
import com.training.event.rabbitMQ.RabbitDomainEventPublisher;
import com.training.serializer.DomainEventSerializer;
import com.training.service.CompanyQueryServiceImpl;
import com.training.service.CompanyServiceImpl;
import com.training.service.EmployeeQueryServiceImpl;
import com.training.service.EmployeeServiceImpl;
import com.training.sql.repository.DbCompanyQueryRepository;
import com.training.sql.repository.DbCompanyRepository;
import com.training.sql.repository.DbEmployeeQueryRepository;
import com.training.sql.repository.DbEmployeeRepository;
import com.training.util.DbConnector;
import com.training.util.RabbitMQConfig;
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
