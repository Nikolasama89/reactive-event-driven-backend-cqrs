package com.itsaur.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.elastic.event.*;
import com.itsaur.elastic.repository.ElasticCompanyQueryRepository;
import com.itsaur.elastic.repository.ElasticEmployeeQueryRepository;
import com.itsaur.elastic.search.ElasticSearchCompanyQueryBuilder;
import com.itsaur.elastic.search.ElasticSearchEmployeeQueryBuilder;
import com.itsaur.event.*;
import com.itsaur.event.db.*;
import com.itsaur.event.rabbitMQ.RabbitConsumer;
import com.itsaur.event.rabbitMQ.RabbitDomainEventPublisher;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.serializer.Serializer;
import com.itsaur.service.CompanyQueryServiceImpl;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeQueryServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.sql.repository.DbCompanyRepository;
import com.itsaur.sql.repository.DbEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import com.itsaur.util.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import io.r2dbc.spi.ConnectionFactory;

import java.util.List;

public class SqlRabbitElastic implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {
        ElasticsearchAsyncClient client = ElasticSearchConfig.getClient();
        ConnectionFactory connectionFactory = DbConnector.getPool();
        Channel publisherChannel = RabbitMQConfig.getChannel();
        String exchange = "domain.events";

        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        var employeeRepository = new DbEmployeeRepository(connectionFactory);
        var companyRepository = new DbCompanyRepository(connectionFactory);
        var companyQueryBuilder = new ElasticSearchCompanyQueryBuilder();
        var employeeQueryBuilder = new ElasticSearchEmployeeQueryBuilder();
        var elasticEmployeeQueryRepository = new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);
        var elasticCompanyQueryRepository = new ElasticCompanyQueryRepository(client, companyQueryBuilder);

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


        List<DomainEventListener<?>> listeners = List.of(
                dbCompanyCreatedEventListener,
                companyCreatedElasticEventListener,
                dbCompanyUpdatedEventListener,
                companyUpdatedElasticEventListener,
                employeeAfterCompanyUpdated,
                dbCompanyDeletedEventListener,
                companyDeletedElasticEventListener,
                dbEmployeeCreatedEventListener,
                employeeCreatedElasticEventListener,
                employeeNameAddToCompanyListener,
                employeeIncrementCompanyTotalEmployeesListener,
                dbEmployeeUpdatedEventListener,
                employeeUpdatedElasticEventListener,
                employeeUpdateNameAtCompanyIndexListener,
                dbEmployeeDeletedEventListener,
                employeeDeletedElasticEventListener,
                employeeDecrementCompanyTotalEmployeesListener,
                employeeDeleteNameFromCompanyIndexListener
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

        Serializer<DomainEvent> domainSerializer = new DomainEventSerializer(serializerMap);
        var eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);
        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new EmployeeQueryServiceImpl(elasticEmployeeQueryRepository);
        var companyQueryService = new CompanyQueryServiceImpl(elasticCompanyQueryRepository);

        companyConsumer.startConsuming();
        employeeConsumer.startConsuming();

        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );

    }
}
