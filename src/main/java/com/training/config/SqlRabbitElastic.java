package com.training.config;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.training.elastic.event.*;
import com.training.elastic.repository.ElasticCompanyQueryRepository;
import com.training.elastic.repository.ElasticEmployeeQueryRepository;
import com.training.elastic.search.ElasticSearchCompanyQueryBuilder;
import com.training.elastic.search.ElasticSearchEmployeeQueryBuilder;
import com.training.event.*;
import com.training.event.db.*;
import com.training.event.rabbitMQ.RabbitConsumer;
import com.training.event.rabbitMQ.RabbitDomainEventPublisher;
import com.training.serializer.DomainEventSerializer;
import com.training.serializer.Serializer;
import com.training.service.CompanyQueryServiceImpl;
import com.training.service.CompanyServiceImpl;
import com.training.service.EmployeeQueryServiceImpl;
import com.training.service.EmployeeServiceImpl;
import com.training.sql.repository.DbCompanyRepository;
import com.training.sql.repository.DbEmployeeRepository;
import com.training.util.DbConnector;
import com.training.util.ElasticSearchConfig;
import com.training.util.RabbitMQConfig;
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
