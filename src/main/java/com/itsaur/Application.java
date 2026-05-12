package com.itsaur;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.dto.*;
import com.itsaur.elastic.event.*;
import com.itsaur.elastic.repository.ElasticCompanyQueryRepository;
import com.itsaur.elastic.repository.ElasticEmployeeQueryRepository;
import com.itsaur.elastic.service.ElasticCompanyQueryServiceImpl;
import com.itsaur.elastic.service.ElasticEmployeeQueryServiceImpl;
import com.itsaur.event.*;
import com.itsaur.event.db.*;
import com.itsaur.event.handler.*;
import com.itsaur.event.kafka.*;
import com.itsaur.event.logging.*;
import com.itsaur.event.rabbitMQ.RabbitConsumer;
import com.itsaur.event.rabbitMQ.RabbitDomainEventPublisher;
import com.itsaur.handler.CompanyHandler;
import com.itsaur.handler.EmployeeHandler;
import com.itsaur.inMemory.repository.InMemoryCompanyQueryRepository;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeQueryRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeRepository;
import com.itsaur.inMemory.search.InMemoryCompanySearch;
import com.itsaur.inMemory.search.InMemoryEmployeeSearch;
import com.itsaur.inMemory.search.InMemoryStorage;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.model.Employee;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.*;
import com.itsaur.elastic.search.ElasticSearchCompanyQueryBuilder;
import com.itsaur.elastic.search.ElasticSearchEmployeeQueryBuilder;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.serializer.*;
import com.itsaur.service.*;
import com.itsaur.sql.repository.DBCompanyQueryRepository;
import com.itsaur.sql.repository.DBCompanyRepository;
import com.itsaur.sql.repository.DBEmployeeQueryRepository;
import com.itsaur.sql.repository.DBEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import com.itsaur.util.KafkaConfig;
import com.itsaur.util.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import io.r2dbc.spi.ConnectionFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;
import java.util.Map;


public class Application {

    public static void main(String[] args) throws Exception {
        String storageType = System.getenv().getOrDefault("STORAGE_TYPE", "sql");

        EmployeeRepository employeeRepository;
        EmployeeQueryRepository employeeQueryRepository;
        CompanyRepository companyRepository;
        CompanyQueryRepository companyQueryRepository;

        EmployeeService employeeService;
        CompanyService companyService;
        EmployeeQueryService employeeQueryService;
        CompanyQueryService companyQueryService;

        EventPublisher eventPublisher;

        if (storageType.equalsIgnoreCase("memory")) {

            DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
            DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
            DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
            DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
            DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
            DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();

            Channel publisherChannel = RabbitMQConfig.getChannel();

            List<DomainEventListener<?>> listeners = List.of(
                    companyCreatedLoggingEventListener,
                    companyUpdatedLoggingListener,
                    companyDeletedLoggingEventListener,
                    employeeCreatedLoggingEventListener,
                    employeeUpdatedLoggingEventListener,
                    employeeDeletedLoggingEventListener
            );

            Map<String, Deserializer<? extends DomainEvent>> deserializerMap = Map.of(
                    "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                    "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                    "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                    "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                    "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                    "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer()
            );

            String exchange = "domain.events";
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

            Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializerMap = Map.of(
                    CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer(),
                    CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer(),
                    CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer(),
                    EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer(),
                    EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer(),
                    EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer()
            );

            Serializer<DomainEvent> domainSerializer =
                    new DomainEventSerializer(serializerMap);

            eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);

            InMemoryStorage storage = new InMemoryStorage();
            InMemoryEmployeeSearch employeeSearch = new InMemoryEmployeeSearch(storage);
            InMemoryCompanySearch companySearch = new InMemoryCompanySearch(storage);

            employeeRepository = new InMemoryEmployeeRepository(storage);
            employeeQueryRepository = new InMemoryEmployeeQueryRepository(storage, employeeSearch);
            companyRepository = new InMemoryCompanyRepository(storage);
            companyQueryRepository = new InMemoryCompanyQueryRepository(storage, companySearch);

            employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
            companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
            employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
            companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

            System.out.println("Running IN-MEMORY");

        } else if (storageType.equalsIgnoreCase("elastic")){

            ElasticsearchAsyncClient client = ElasticSearchConfig.getClient();
            ConnectionFactory connectionFactory = DbConnector.getPool();

            employeeRepository = new DBEmployeeRepository(connectionFactory);
            companyRepository = new DBCompanyRepository(connectionFactory);

            ElasticSearchCompanyQueryBuilder companyQueryBuilder = new ElasticSearchCompanyQueryBuilder();
            ElasticSearchEmployeeQueryBuilder employeeQueryBuilder = new ElasticSearchEmployeeQueryBuilder();

            ElasticEmployeeQueryRepository elasticEmployeeQueryRepository =
                    new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);

            ElasticCompanyQueryRepository elasticCompanyQueryRepository =
                    new ElasticCompanyQueryRepository(client, companyQueryBuilder);

            CompanyCreatedLoggingEventListener companyCreatedLoggingEventListener =
                    new CompanyCreatedLoggingEventListener();

            CompanyUpdatedLoggingEventListener companyUpdatedLoggingListener =
                    new CompanyUpdatedLoggingEventListener();

            CompanyDeletedLoggingEventListener companyDeletedLoggingEventListener =
                    new CompanyDeletedLoggingEventListener();

            EmployeeCreatedLoggingEventListener employeeCreatedLoggingEventListener =
                    new EmployeeCreatedLoggingEventListener();

            EmployeeUpdatedLoggingEventListener employeeUpdatedLoggingEventListener =
                    new EmployeeUpdatedLoggingEventListener();

            EmployeeDeletedLoggingEventListener employeeDeletedLoggingEventListener =
                    new EmployeeDeletedLoggingEventListener();

            DBCompanyCreatedEventListener dbCompanyCreatedEventListener =
                    new DBCompanyCreatedEventListener(companyRepository);

            DBCompanyUpdatedEventListener dbCompanyUpdatedEventListener =
                    new DBCompanyUpdatedEventListener(companyRepository);

            DBCompanyDeletedEventListener dbCompanyDeletedEventListener =
                    new DBCompanyDeletedEventListener(companyRepository);

            DBEmployeeCreatedEventListener dbEmployeeCreatedEventListener =
                    new DBEmployeeCreatedEventListener(employeeRepository);

            DBEmployeeUpdatedEventListener dbEmployeeUpdatedEventListener =
                    new DBEmployeeUpdatedEventListener(employeeRepository);

            DBEmployeeDeletedEventListener dbEmployeeDeletedEventListener =
                    new DBEmployeeDeletedEventListener(employeeRepository);

            CompanyCreatedElasticEventListener companyCreatedElasticEventListener =
                    new CompanyCreatedElasticEventListener(client);

            CompanyUpdatedElasticEventListener companyUpdatedElasticEventListener =
                    new CompanyUpdatedElasticEventListener(client);

            CompanyDeletedElasticEventListener companyDeletedElasticEventListener =
                    new CompanyDeletedElasticEventListener(client);

            EmployeeAfterCompanyUpdatedElasticEventListener employeeAfterCompanyUpdated =
                    new EmployeeAfterCompanyUpdatedElasticEventListener(client);

            EmployeeCreatedElasticEventListener employeeCreatedElasticEventListener =
                    new EmployeeCreatedElasticEventListener(client);

            EmployeeUpdatedElasticEventListener employeeUpdatedElasticEventListener =
                    new EmployeeUpdatedElasticEventListener(client);

            EmployeeDeletedElasticEventListener employeeDeletedElasticEventListener =
                    new EmployeeDeletedElasticEventListener(client);

            EmployeeCreateIncrementTotalEmployeesListener employeeIncrementCompanyTotalEmployeesListener =
                    new EmployeeCreateIncrementTotalEmployeesListener(client);

            EmployeeDeletedDecrementTotalEmployeesListener employeeDecrementCompanyTotalEmployeesListener =
                    new EmployeeDeletedDecrementTotalEmployeesListener(client);

            IndexAddEmployeeNameToCompanyElasticEventListener employeeNameAddToCompanyListener =
                    new IndexAddEmployeeNameToCompanyElasticEventListener(client);

            IndexRemoveEmployeeFromCompanyElasticEventListener employeeDeleteNameFromCompanyIndexListener =
                    new IndexRemoveEmployeeFromCompanyElasticEventListener(client);

            IndexUpdateEmployeeInCompanyElasticEventListener employeeUpdateNameAtCompanyIndexListener =
                    new IndexUpdateEmployeeInCompanyElasticEventListener(client);

            DBCompanyCreatedEventHandler DBCompanyCreatedEventHandler = new DBCompanyCreatedEventHandler(
                    dbCompanyCreatedEventListener,
                    companyCreatedLoggingEventListener
            );

            DBCompanyUpdatedEventHandler DBCompanyUpdatedEventHandler = new DBCompanyUpdatedEventHandler(
                    dbCompanyUpdatedEventListener,
                    companyUpdatedLoggingListener
            );

            DBCompanyDeletedEventHandler DBCompanyDeletedEventHandler = new DBCompanyDeletedEventHandler(
                    dbCompanyDeletedEventListener,
                    companyDeletedLoggingEventListener
            );

            DBEmployeeCreatedEventHandler DBEmployeeCreatedEventHandler = new DBEmployeeCreatedEventHandler(
                    dbEmployeeCreatedEventListener,
                    employeeCreatedLoggingEventListener
            );

            DBEmployeeUpdatedEventHandler DBEmployeeUpdatedEventHandler = new DBEmployeeUpdatedEventHandler(
                    dbEmployeeUpdatedEventListener,
                    employeeUpdatedLoggingEventListener
            );

            DBEmployeeDeletedEventHandler DBEmployeeDeletedEventHandler = new DBEmployeeDeletedEventHandler(
                    dbEmployeeDeletedEventListener,
                    employeeDeletedLoggingEventListener
            );

            ElasticCompanyCreatedEventHandler elasticCompanyCreatedEventHandler = new ElasticCompanyCreatedEventHandler(
                    companyCreatedElasticEventListener
            );

            ElasticCompanyUpdatedEventHandler elasticCompanyUpdatedEventHandler = new ElasticCompanyUpdatedEventHandler(
                    companyUpdatedElasticEventListener,
                    employeeAfterCompanyUpdated

            );

            ElasticCompanyDeletedEventHandler elasticCompanyDeletedEventHandler = new ElasticCompanyDeletedEventHandler(
                    companyDeletedElasticEventListener
            );

            ElasticEmployeeCreatedEventHandler elasticEmployeeCreatedEventHandler = new ElasticEmployeeCreatedEventHandler(
                    employeeCreatedElasticEventListener,
                    employeeIncrementCompanyTotalEmployeesListener,
                    employeeNameAddToCompanyListener
            );

            ElasticEmployeeUpdatedEventHandler elasticEmployeeUpdatedEventHandler = new ElasticEmployeeUpdatedEventHandler(
                    employeeUpdatedElasticEventListener,
                    employeeUpdateNameAtCompanyIndexListener
            );

            ElasticEmployeeDeletedEventHandler elasticEmployeeDeletedEventHandler = new ElasticEmployeeDeletedEventHandler(
                    employeeDeletedElasticEventListener,
                    employeeDecrementCompanyTotalEmployeesListener,
                    employeeDeleteNameFromCompanyIndexListener
            );

            Map<Class<? extends DomainEvent>, DomainEventListener<?>> dbHandlers = Map.of(
                    CompanyCreatedEvent.class, DBCompanyCreatedEventHandler,
                    CompanyUpdatedEvent.class, DBCompanyUpdatedEventHandler,
                    CompanyDeletedEvent.class, DBCompanyDeletedEventHandler,
                    EmployeeCreatedEvent.class, DBEmployeeCreatedEventHandler,
                    EmployeeUpdatedEvent.class, DBEmployeeUpdatedEventHandler,
                    EmployeeDeletedEvent.class, DBEmployeeDeletedEventHandler
            );

            Map<Class<? extends DomainEvent>, DomainEventListener<?>> elasticHandlers = Map.of(
                    CompanyCreatedEvent.class, elasticCompanyCreatedEventHandler,
                    CompanyUpdatedEvent.class, elasticCompanyUpdatedEventHandler,
                    CompanyDeletedEvent.class, elasticCompanyDeletedEventHandler,
                    EmployeeCreatedEvent.class, elasticEmployeeCreatedEventHandler,
                    EmployeeUpdatedEvent.class, elasticEmployeeUpdatedEventHandler,
                    EmployeeDeletedEvent.class, elasticEmployeeDeletedEventHandler
            );

            String bootstrapServers = "localhost:9092";
            String topic = "domain-events";

            KafkaSender<String, byte[]> sender =
                    KafkaConfig.createProducer(bootstrapServers);

            KafkaReceiver<String, byte[]> dbReceiver =
                    KafkaConfig.createReceiver(bootstrapServers, topic, "db-consumer");

            KafkaReceiver<String, byte[]> elasticReceiver =
                    KafkaConfig.createReceiver(bootstrapServers, topic, "elastic-consumer");

            Map<String, Deserializer<? extends DomainEvent>> deserializerMap = Map.of(
                    "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                    "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                    "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                    "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                    "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                    "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer()
            );

            KafkaDomainEventConsumer dbConsumer =
                    new KafkaDomainEventConsumer(
                            dbReceiver,
                            dbHandlers,
                            deserializerMap
                    );

            KafkaDomainEventConsumer elasticConsumer =
                    new KafkaDomainEventConsumer(
                            elasticReceiver,
                            elasticHandlers,
                            deserializerMap
                    );

            dbConsumer.startConsuming();
            elasticConsumer.startConsuming();

            Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializerMap = Map.of(
                    CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer(),
                    CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer(),
                    CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer(),
                    EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer(),
                    EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer(),
                    EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer()
            );

            Serializer<DomainEvent> domainSerializer =
                    new DomainEventSerializer(serializerMap);

            eventPublisher = new KafkaDomainEventPublisher(sender, topic, domainSerializer);

            employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
            companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
            employeeQueryService = new ElasticEmployeeQueryServiceImpl(elasticEmployeeQueryRepository);
            companyQueryService = new ElasticCompanyQueryServiceImpl(elasticCompanyQueryRepository);

            System.out.println("Running SQL with ELASTIC");
        }

        else {
            ConnectionFactory connectionFactory = DbConnector.getPool();

            DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
            DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
            DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
            DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
            DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
            DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();

            Channel publisherChannel = RabbitMQConfig.getChannel();
            String exchange = "domain.events";

            List<DomainEventListener<?>> listeners = List.of(
                    companyCreatedLoggingEventListener,
                    companyUpdatedLoggingListener,
                    companyDeletedLoggingEventListener,
                    employeeCreatedLoggingEventListener,
                    employeeUpdatedLoggingEventListener,
                    employeeDeletedLoggingEventListener
            );

            Map<String, Deserializer<? extends DomainEvent>> deserializerMap = Map.of(
                    "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                    "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                    "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                    "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                    "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                    "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer()
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

            Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializerMap = Map.of(
                    CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer(),
                    CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer(),
                    CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer(),
                    EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer(),
                    EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer(),
                    EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer()
            );

            Serializer<DomainEvent> domainSerializer =
                    new DomainEventSerializer(serializerMap);

            eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);

            employeeRepository = new DBEmployeeRepository(connectionFactory);
            employeeQueryRepository = new DBEmployeeQueryRepository(connectionFactory);
            companyRepository = new DBCompanyRepository(connectionFactory);
            companyQueryRepository = new DBCompanyQueryRepository(connectionFactory);

            employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
            companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
            employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
            companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

            System.out.println("Running with SQL storage");
        }

        // Serializers/Deserializers
        Serializer<Employee> employeeSerializer = new JSONEmployeeSerializer();
        Serializer<EmployeeQueryModel> employeeQueryModelSerializer = new JSONEmployeeQueryModel();
        Serializer<List<EmployeeQueryModel>> employeeQueryListModelSerializer = new JSONEmployeeQueryModelList();
        Serializer<PageDTO<EmployeeQueryModel>> employeePageSerializer = new JSONEmployeeQueryModelPageSerializer();
        Deserializer<EmployeeCreateDTO> employeeCreate = new JSONEmployeeCreateDeserializer();
        Deserializer<EmployeeUpdateDTO> employeeUpdate = new JSONEmployeeUpdateDeserializer();
        Serializer<CreateResponseDTO> createResponseDTOSerializer = new JSONCreateResponseSerializer();
        Serializer<Company> companySerializer = new JSONCompanySerializer();
        Serializer<CompanyQueryModel> companyQueryModelSerializer = new JSONCompanyQueryModelSerializer();
        Serializer<List<CompanyQueryModel>> companyQueryModelListSerializer= new JSONCompanyQueryModelListSerializer();
        Serializer<PageDTO<CompanyQueryModel>> companyQueryModelPageSerializer= new JSONCompanyQueryModelPageSerializer();
        Deserializer<CompanyCreateDTO> companyCreate = new JSONCompanyCreateDeserializer();
        Deserializer<CompanyUpdateDTO> companyUpdate = new JSONCompanyUpdateDeserializer();
        Deserializer<SearchComponent> searchRequestDTODeserializer = new JSONSearchComponentQueryDeserializer();

        // Handlers
        EmployeeHandler employeeHandler = new EmployeeHandler(
                employeeService,
                employeeQueryService,
                employeeSerializer,
                employeeCreate,
                employeeUpdate,
                createResponseDTOSerializer,
                employeeQueryModelSerializer,
                employeeQueryListModelSerializer,
                employeePageSerializer,
                searchRequestDTODeserializer
        );

        CompanyHandler companyHandler = new CompanyHandler(
                companyService,
                companyQueryService,
                companySerializer,
                companyCreate,
                companyUpdate,
                createResponseDTOSerializer,
                companyQueryModelSerializer,
                companyQueryModelListSerializer,
                companyQueryModelPageSerializer,
                searchRequestDTODeserializer
        );

        DisposableServer server = HttpServer.create()
                .host("127.0.0.1")
                .port(8080)
                .route(routes -> {
                    employeeHandler.employeeRoutes(routes);
                    companyHandler.companyRoutes(routes);
                })
                .bindNow();
        server.onDispose().block();
    }
}