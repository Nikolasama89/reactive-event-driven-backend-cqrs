package com.itsaur;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.dto.*;
import com.itsaur.elastic.event.*;
import com.itsaur.elastic.repository.ElasticCompanyQueryRepository;
import com.itsaur.elastic.repository.ElasticEmployeeQueryRepository;
import com.itsaur.elastic.service.ElasticCompanyQueryServiceImpl;
import com.itsaur.elastic.service.ElasticEmployeeQueryServiceImpl;
import com.itsaur.event.*;
//import com.itsaur.event.db.*;
import com.itsaur.event.kafka.*;
import com.itsaur.event.logging.*;
import com.itsaur.event.rabbitMQ.RabbitConsumer;
import com.itsaur.event.rabbitMQ.RabbitDomainEventPublisher;
import com.itsaur.handler.CompanyHandler;
import com.itsaur.handler.EmployeeHandler;
import com.itsaur.inMemory.repository.InMemoryCompanyQueryRepository;
//import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeQueryRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeRepository;
import com.itsaur.inMemory.search.InMemoryCompanySearch;
import com.itsaur.inMemory.search.InMemoryEmployeeSearch;
import com.itsaur.inMemory.search.InMemoryStorage;
import com.itsaur.kafkaStreams.CompanyTopologyBuilder;
import com.itsaur.kafkaStreams.EmployeeTopologyBuilder;
import com.itsaur.kafkaStreams.StreamCompanyRepository;
import com.itsaur.kafkaStreams.StreamEmployeeRepository;
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
import com.itsaur.sql.repository.DbCompanyQueryRepository;
import com.itsaur.sql.repository.DbCompanyRepository;
import com.itsaur.sql.repository.DbEmployeeQueryRepository;
import com.itsaur.sql.repository.DbEmployeeRepository;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import com.itsaur.util.KafkaConfig;
import com.itsaur.util.RabbitMQConfig;
import com.rabbitmq.client.Channel;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import reactor.kafka.sender.KafkaSender;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;
import java.util.Map;
import java.util.Properties;


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

        Map<Class<? extends DomainEvent>, Serializer<? extends DomainEvent>> serializerMap = Map.of(
                CompanyCreatedEvent.class, new JSONCompanyCreatedEventSerializer(),
                CompanyUpdatedEvent.class, new JSONCompanyUpdatedEventSerializer(),
                CompanyDeletedEvent.class, new JSONCompanyDeletedEventSerializer(),
                EmployeeCreatedEvent.class, new JSONEmployeeCreatedEventSerializer(),
                EmployeeUpdatedEvent.class, new JSONEmployeeUpdatedEventSerializer(),
                EmployeeDeletedEvent.class, new JSONEmployeeDeletedEventSerializer()
        );

        Map<String, Deserializer<? extends DomainEvent>> deserializerMap = Map.of(
                "CompanyCreatedEvent", new JSONCompanyCreatedEventDeserializer(),
                "CompanyUpdatedEvent", new JSONCompanyUpdatedEventDeserializer(),
                "CompanyDeletedEvent", new JSONCompanyDeletedEventDeserializer(),
                "EmployeeCreatedEvent", new JSONEmployeeCreatedEventDeserializer(),
                "EmployeeUpdatedEvent", new JSONEmployeeUpdatedEventDeserializer(),
                "EmployeeDeletedEvent", new JSONEmployeeDeletedEventDeserializer()
        );

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
//            ConnectionFactory connectionFactory = DbConnector.getPool();

            String bootstrapServers = "localhost:9092";

            Properties companyProps = new Properties();
            companyProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "company-streams-app");
            companyProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            companyProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            companyProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

            Properties employeeProps = new Properties();
            employeeProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-streams-app");
            employeeProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            employeeProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            employeeProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

            Serde<DomainEvent> eventSerde = Serdes.serdeFrom(
                    new StreamDomainEventSerializer(), new StreamDomainEventDeserializer());
            Serde<Company> companySerde = Serdes.serdeFrom(
                    new StreamCompanySerializer(), new StreamCompanyDeserializer());
            Serde<Employee> employeeSerde = Serdes.serdeFrom(
                    new StreamEmployeeSerializer(), new StreamEmployeeDeserializer()
            );

            System.out.println(eventSerde.deserializer().getClass());

            Topology companyTopology = new CompanyTopologyBuilder(eventSerde, companySerde).build();
            Topology employeeTopology = new EmployeeTopologyBuilder(eventSerde, employeeSerde).build();

            KafkaStreams companyStreams = new KafkaStreams(companyTopology, companyProps);
            KafkaStreams employeeStreams = new KafkaStreams(employeeTopology, employeeProps);
            companyStreams.start();
            employeeStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(companyStreams::close));
            Runtime.getRuntime().addShutdownHook(new Thread(employeeStreams::close));


            employeeRepository = new StreamEmployeeRepository(employeeStreams);
            companyRepository = new StreamCompanyRepository(companyStreams);

            ElasticSearchCompanyQueryBuilder companyQueryBuilder = new ElasticSearchCompanyQueryBuilder();
            ElasticSearchEmployeeQueryBuilder employeeQueryBuilder = new ElasticSearchEmployeeQueryBuilder();

            ElasticEmployeeQueryRepository elasticEmployeeQueryRepository =
                    new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);

            ElasticCompanyQueryRepository elasticCompanyQueryRepository =
                    new ElasticCompanyQueryRepository(client, companyQueryBuilder);


//            DomainEventListener<CompanyCreatedEvent> dbCompanyCreatedEventListener =
//                    new DbCompanyCreatedEventListener(companyReadRepository);

//            DomainEventListener<CompanyUpdatedEvent> dbCompanyUpdatedEventListener =
//                    new DbCompanyUpdatedEventListener(companyReadRepository);

//            DomainEventListener<CompanyDeletedEvent> dbCompanyDeletedEventListener =
//                    new DbCompanyDeletedEventListener(companyReadRepository);

//            DomainEventListener<EmployeeCreatedEvent> dbEmployeeCreatedEventListener =
//                    new DbEmployeeCreatedEventListener(employeeRepository);

//            DomainEventListener<EmployeeUpdatedEvent> dbEmployeeUpdatedEventListener =
//                    new DbEmployeeUpdatedEventListener(employeeRepository);

//            DomainEventListener<EmployeeDeletedEvent> dbEmployeeDeletedEventListener =
//                    new DbEmployeeDeletedEventListener(employeeRepository);

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
//                    dbCompanyCreatedEventListener,
//                    dbCompanyUpdatedEventListener,
//                    dbCompanyDeletedEventListener,
//                    dbEmployeeCreatedEventListener,
//                    dbEmployeeUpdatedEventListener,
//                    dbEmployeeDeletedEventListener
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


            String topic = "domain-events";

            KafkaSender<String, byte[]> sender =
                    KafkaConfig.createProducer(bootstrapServers);

//            KafkaReceiver<String, byte[]> dbReceiver =
//                    KafkaConfig.createReceiver(bootstrapServers, topic, "db-consumer");
//
//            KafkaReceiver<String, byte[]> elasticReceiver =
//                    KafkaConfig.createReceiver(bootstrapServers, topic, "elastic-consumer");



//            KafkaDomainEventConsumer dbConsumer =
//                    new KafkaDomainEventConsumer(
//                            dbReceiver,
//                            dbListeners,
//                            deserializerMap
//                    );
//
//            KafkaDomainEventConsumer elasticConsumer =
//                    new KafkaDomainEventConsumer(
//                            elasticReceiver,
//                            elasticListeners,
//                            deserializerMap
//                    );

//            dbConsumer.startConsuming();
//            elasticConsumer.startConsuming();


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


            Serializer<DomainEvent> domainSerializer =
                    new DomainEventSerializer(serializerMap);

            eventPublisher = new RabbitDomainEventPublisher(publisherChannel, exchange, domainSerializer);

            employeeRepository = new DbEmployeeRepository(connectionFactory);
            employeeQueryRepository = new DbEmployeeQueryRepository(connectionFactory);
            companyRepository = new DbCompanyRepository(connectionFactory);
            companyQueryRepository = new DbCompanyQueryRepository(connectionFactory);

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