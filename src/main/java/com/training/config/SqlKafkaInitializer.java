package com.training.config;

import com.training.event.*;
import com.training.event.db.*;
import com.training.event.kafka.KafkaDomainEventConsumer;
import com.training.event.kafka.KafkaDomainEventPublisher;
import com.training.serializer.DomainEventSerializer;
import com.training.serializer.Serializer;
import com.training.service.CompanyQueryServiceImpl;
import com.training.service.CompanyServiceImpl;
import com.training.service.EmployeeQueryServiceImpl;
import com.training.service.EmployeeServiceImpl;
import com.training.sql.repository.DbCompanyQueryRepository;
import com.training.sql.repository.DbCompanyRepository;
import com.training.sql.repository.DbEmployeeQueryRepository;
import com.training.sql.repository.DbEmployeeRepository;
import com.training.util.DbConnector;
import com.training.util.KafkaConfig;
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
