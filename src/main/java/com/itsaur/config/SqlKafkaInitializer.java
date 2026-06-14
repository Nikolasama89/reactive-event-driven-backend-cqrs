package com.itsaur.config;

import com.itsaur.event.*;
import com.itsaur.event.db.*;
import com.itsaur.event.kafka.KafkaDomainEventConsumer;
import com.itsaur.event.kafka.KafkaDomainEventPublisher;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.serializer.Serializer;
import com.itsaur.service.CompanyQueryServiceImpl;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeQueryServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.sql.repository.DbCompanyQueryRepository;
import com.itsaur.sql.repository.DbCompanyRepository;
import com.itsaur.sql.repository.DbEmployeeQueryRepository;
import com.itsaur.sql.repository.DbEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.KafkaConfig;
import io.r2dbc.spi.ConnectionFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

import java.util.List;

public class SqlKafkaInitializer implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {
        ConnectionFactory connectionFactory = DbConnector.getPool();
        String bootstrapServers = "localhost:9092";
        String topic = "domain-events";

        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        var employeeRepository = new DbEmployeeRepository(connectionFactory);
        var employeeQueryRepository = new DbEmployeeQueryRepository(connectionFactory);
        var companyRepository = new DbCompanyRepository(connectionFactory);
        var companyQueryRepository = new DbCompanyQueryRepository(connectionFactory);

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


        List<DomainEventListener<?>> dbListeners = List.of(
                dbCompanyCreatedEventListener,
                dbCompanyUpdatedEventListener,
                dbCompanyDeletedEventListener,
                dbEmployeeCreatedEventListener,
                dbEmployeeUpdatedEventListener,
                dbEmployeeDeletedEventListener
        );

        KafkaSender<String, byte[]> sender =
                KafkaConfig.createProducer(bootstrapServers);

        KafkaReceiver<String, byte[]> dbReceiver =
                KafkaConfig.createReceiver(bootstrapServers, topic, "db-consumer");


        KafkaDomainEventConsumer dbConsumer = new KafkaDomainEventConsumer(dbReceiver, dbListeners, deserializerMap);

        dbConsumer.startConsuming();

        Serializer<DomainEvent> domainSerializer = new DomainEventSerializer(serializerMap);

        var eventPublisher = new KafkaDomainEventPublisher(sender, topic, domainSerializer);
        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
        var companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
