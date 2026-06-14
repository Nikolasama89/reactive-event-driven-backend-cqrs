package com.itsaur.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.elastic.event.*;
import com.itsaur.elastic.repository.ElasticCompanyQueryRepository;
import com.itsaur.elastic.repository.ElasticEmployeeQueryRepository;
import com.itsaur.elastic.search.ElasticSearchCompanyQueryBuilder;
import com.itsaur.elastic.search.ElasticSearchEmployeeQueryBuilder;
import com.itsaur.elastic.service.ElasticCompanyQueryServiceImpl;
import com.itsaur.elastic.service.ElasticEmployeeQueryServiceImpl;
import com.itsaur.event.*;
import com.itsaur.event.db.*;
import com.itsaur.event.kafka.KafkaDomainEventConsumer;
import com.itsaur.event.kafka.KafkaDomainEventPublisher;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.serializer.Serializer;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.sql.repository.DbCompanyRepository;
import com.itsaur.sql.repository.DbEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import com.itsaur.util.KafkaConfig;
import io.r2dbc.spi.ConnectionFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

import java.util.List;

public class SqlKafkaElasticInitializer implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {
        ElasticsearchAsyncClient client = ElasticSearchConfig.getClient();
        ConnectionFactory connectionFactory = DbConnector.getPool();
        String bootstrapServers = "localhost:9092";
        String topic = "domain-events";

        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        var employeeRepository = new DbEmployeeRepository(connectionFactory);
        var companyRepository = new DbCompanyRepository(connectionFactory);
        var companyQueryBuilder = new ElasticSearchCompanyQueryBuilder();
        var employeeQueryBuilder = new ElasticSearchEmployeeQueryBuilder();
        var elasticEmployeeQueryRepository = new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);
        var  elasticCompanyQueryRepository = new ElasticCompanyQueryRepository(client, companyQueryBuilder);

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

        DomainEventListener<CompanyCreatedEvent> companyCreatedElasticEventListener =
                new CompanyCreatedElasticEventListener(client);

        DomainEventListener<CompanyUpdatedEvent> companyUpdatedElasticEventListener =
                new CompanyUpdatedElasticEventListener(client);

        DomainEventListener<CompanyDeletedEvent> companyDeletedElasticEventListener =
                new CompanyDeletedElasticEventListener(client);

        DomainEventListener<CompanyUpdatedEvent> employeeAfterCompanyUpdated =
                new EmployeeAfterCompanyUpdatedElasticEventListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeCreatedElasticEventListener =
                new EmployeeCreatedElasticEventListener(client);

        DomainEventListener<EmployeeUpdatedEvent> employeeUpdatedElasticEventListener =
                new EmployeeUpdatedElasticEventListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDeletedElasticEventListener =
                new EmployeeDeletedElasticEventListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeIncrementCompanyTotalEmployeesListener =
                new EmployeeCreateIncrementTotalEmployeesListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDecrementCompanyTotalEmployeesListener =
                new EmployeeDeletedDecrementTotalEmployeesListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeNameAddToCompanyListener =
                new IndexAddEmployeeNameToCompanyElasticEventListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDeleteNameFromCompanyIndexListener =
                new IndexRemoveEmployeeFromCompanyElasticEventListener(client);

        DomainEventListener<EmployeeUpdatedEvent> employeeUpdateNameAtCompanyIndexListener =
                new IndexUpdateEmployeeInCompanyElasticEventListener(client);


        List<DomainEventListener<?>> dbListeners = List.of(
                dbCompanyCreatedEventListener,
                dbCompanyUpdatedEventListener,
                dbCompanyDeletedEventListener,
                dbEmployeeCreatedEventListener,
                dbEmployeeUpdatedEventListener,
                dbEmployeeDeletedEventListener
        );

        List<DomainEventListener<?>> elasticListeners = List.of(
                companyCreatedElasticEventListener,
                companyUpdatedElasticEventListener,
                employeeUpdateNameAtCompanyIndexListener,
                employeeAfterCompanyUpdated,
                companyDeletedElasticEventListener,
                employeeCreatedElasticEventListener,
                employeeIncrementCompanyTotalEmployeesListener,
                employeeNameAddToCompanyListener,
                employeeUpdatedElasticEventListener,
                employeeDeletedElasticEventListener,
                employeeDecrementCompanyTotalEmployeesListener,
                employeeDeleteNameFromCompanyIndexListener
        );


        KafkaSender<String, byte[]> sender =
                KafkaConfig.createProducer(bootstrapServers);

        KafkaReceiver<String, byte[]> dbReceiver =
                KafkaConfig.createReceiver(bootstrapServers, topic, "db-consumer");

        KafkaReceiver<String, byte[]> elasticReceiver =
                KafkaConfig.createReceiver(bootstrapServers, topic, "elastic-consumer");

        KafkaDomainEventConsumer dbConsumer = new KafkaDomainEventConsumer(dbReceiver, dbListeners, deserializerMap);
        KafkaDomainEventConsumer elasticConsumer = new KafkaDomainEventConsumer(elasticReceiver, elasticListeners, deserializerMap);

        dbConsumer.startConsuming();
        elasticConsumer.startConsuming();

        Serializer<DomainEvent> domainSerializer = new DomainEventSerializer(serializerMap);

        var eventPublisher = new KafkaDomainEventPublisher(sender, topic, domainSerializer);
        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new ElasticEmployeeQueryServiceImpl(elasticEmployeeQueryRepository);
        var companyQueryService = new ElasticCompanyQueryServiceImpl(elasticCompanyQueryRepository);

        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
