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
import com.itsaur.event.logging.*;
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

            DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
            DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
            DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
            DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
            DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
            DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();
            DomainEventListener<?> dbCompanyCreatedEventListener = new DBCompanyCreatedEventListener(companyRepository);
            DomainEventListener<?> dbCompanyUpdatedEventListener = new DBCompanyUpdatedEventListener(companyRepository);
            DomainEventListener<?> dbCompanyDeletedEventListener = new DBCompanyDeletedEventListener(companyRepository);
            DomainEventListener<?> dbEmployeeCreatedEventListener = new DBEmployeeCreatedEventListener(employeeRepository);
            DomainEventListener<?> dbEmployeeUpdatedEventListener = new DBEmployeeUpdatedEventListener(employeeRepository);
            DomainEventListener<?> dbEmployeeDeletedEventListener = new DBEmployeeDeletedEventListener(employeeRepository);
            DomainEventListener<?> companyCreatedElasticEventListener =
                    new CompanyCreatedElasticEventListener(client);
            DomainEventListener<?> companyUpdatedElasticEventListener =
                    new CompanyUpdatedElasticEventListener(client);
            DomainEventListener<?> companyDeletedElasticEventListener =
                    new CompanyDeletedElasticEventListener(client);
            DomainEventListener<?> employeeCreatedElasticEventListener =
                    new EmployeeCreatedElasticEventListener(client);
            DomainEventListener<?> employeeUpdatedElasticEventListener =
                    new EmployeeUpdatedElasticEventListener(client);
            DomainEventListener<?> employeeDeletedElasticEventListener =
                    new EmployeeDeletedElasticEventListener(client);
            DomainEventListener<?> employeeIncrementCompanyTotalEmployeesListener =
                    new EmployeeCreateIncrementTotalEmployeesListener(client);
            DomainEventListener<?> employeeDecrementCompanyTotalEmployeesListener =
                    new EmployeeDeletedDecrementTotalEmployeesListener(client);
            DomainEventListener<?> employeeNameAddToCompanyListener =
                    new IndexAddEmployeeNameToCompanyElasticEventListener(client);
            DomainEventListener<?> employeeDeleteNameFromCompanyIndexListener =
                    new IndexRemoveEmployeeFromCompanyElasticEventListener(client);
            DomainEventListener<?> employeeUpdateNameAtCompanyIndexListener =
                    new IndexUpdateEmployeeInCompanyElasticEventListener(client);

            String bootstrapServers = "localhost:9092";
            String topic = "domain-events";

            KafkaSender<String, byte[]> sender =
                    KafkaConfig.createProducer(bootstrapServers);

            KafkaReceiver<String, byte[]> receiver =
                    KafkaConfig.createReceiver(bootstrapServers, topic);

//            Channel publisherChannel = RabbitMQConfig.getChannel();
//            Channel consumerChannel = RabbitMQConfig.getChannel();
//            String exchange = "domain.events";

            List<DomainEventListener<?>> listeners = List.of(
                    dbCompanyCreatedEventListener,
                    companyCreatedLoggingEventListener,
                    companyCreatedElasticEventListener,
                    dbCompanyUpdatedEventListener,
                    companyUpdatedLoggingListener,
                    companyUpdatedElasticEventListener,
                    dbCompanyDeletedEventListener,
                    companyDeletedLoggingEventListener,
                    companyDeletedElasticEventListener,
                    dbEmployeeCreatedEventListener,
                    employeeCreatedLoggingEventListener,
                    employeeCreatedElasticEventListener,
                    dbEmployeeUpdatedEventListener,
                    employeeUpdatedLoggingEventListener,
                    employeeUpdatedElasticEventListener,
                    dbEmployeeDeletedEventListener,
                    employeeDeletedLoggingEventListener,
                    employeeDeletedElasticEventListener,
                    employeeIncrementCompanyTotalEmployeesListener,
                    employeeDecrementCompanyTotalEmployeesListener,
                    employeeNameAddToCompanyListener,
                    employeeDeleteNameFromCompanyIndexListener,
                    employeeUpdateNameAtCompanyIndexListener
            );

            Map<String, Deserializer<? extends DomainEvent>> deserializerMap = Map.of(
                    "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                    "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                    "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                    "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                    "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                    "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer()
            );

            KafkaDomainEventConsumer kafkaConsumer =
                    new KafkaDomainEventConsumer(receiver, listeners, deserializerMap);

            kafkaConsumer.startConsuming();

//            RabbitConsumer companyConsumer = new RabbitConsumer(
//                    consumerChannel,
//                    "company.queue",
//                    listeners,
//                    deserializerMap,
//                    exchange,
//                    new String[]{"CompanyCreatedEvent", "CompanyUpdatedEvent", "CompanyDeletedEvent"}
//            );
//
//            RabbitConsumer employeeConsumer = new RabbitConsumer(
//                    consumerChannel,
//                    "employee.queue",
//                    listeners,
//                    deserializerMap,
//                    exchange,
//                    new String[]{"EmployeeCreatedEvent", "EmployeeUpdatedEvent", "EmployeeDeletedEvent"});
//
//            companyConsumer.startConsuming();
//            employeeConsumer.startConsuming();

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

//            eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);

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