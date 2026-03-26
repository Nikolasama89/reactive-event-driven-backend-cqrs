package com.itsaur;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.itsaur.dto.*;
import com.itsaur.event.*;
import com.itsaur.handler.CompanyHandler;
import com.itsaur.handler.EmployeeHandler;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.model.Employee;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.*;
import com.itsaur.repository.elastic_events.*;
import com.itsaur.repository.search.ElasticSearchCompanyQueryBuilder;
import com.itsaur.repository.search.ElasticSearchEmployeeQueryBuilder;
import com.itsaur.serializer.*;
import com.itsaur.service.*;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import io.r2dbc.spi.ConnectionFactory;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;


public class Application {

    public static void main(String[] args) {
        String storageType = System.getenv().getOrDefault("STORAGE_TYPE", "sql");

        EmployeeRepository employeeRepository;
        EmployeeQueryRepository employeeQueryRepository;
        CompanyRepository companyRepository;
        CompanyQueryRepository companyQueryRepository;

        EmployeeService employeeService;
        CompanyService companyService;
        EmployeeQueryService employeeQueryService;
        CompanyQueryService companyQueryService;

        if (storageType.equalsIgnoreCase("memory")) {

            InMemoryStorage storage = new InMemoryStorage();
            InMemoryEmployeeSearch employeeSearch = new InMemoryEmployeeSearch(storage);
            InMemoryCompanySearch companySearch = new InMemoryCompanySearch(storage);

            employeeRepository = new InMemoryEmployeeRepository(storage);
            employeeQueryRepository = new InMemoryEmployeeQueryRepository(storage, employeeSearch);
            companyRepository = new InMemoryCompanyRepository(storage);
            companyQueryRepository = new InMemoryCompanyQueryRepository(storage, companySearch);

            employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository);
            companyService = new CompanyServiceImpl(companyRepository);
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

            employeeQueryRepository = new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);
            companyQueryRepository = new ElasticCompanyQueryRepository(client, companyQueryBuilder);
            ElasticEmployeeRepository elasticEmployeeRepository = new ElasticEmployeeRepository(client, companyRepository);
            ElasticCompanyRepository elasticCompanyRepository = new ElasticCompanyRepository(client);

            DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
            DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
            DomainEventListener<?> companyCreatedElasticEventListener =
                    new CompanyCreatedElasticEventListener(companyRepository, elasticCompanyRepository);
            DomainEventListener<?> companyUpdatedElasticEventListener =
                    new CompanyUpdatedElasticEventListener(companyRepository, elasticCompanyRepository);
            DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
            DomainEventListener<?> companyDeletedElasticEventListener = new CompanyDeletedElasticEventListener(elasticCompanyRepository);
            DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
            DomainEventListener<?> employeeCreatedElasticEventListener = new EmployeeCreatedElasticEventListener(employeeRepository, elasticEmployeeRepository);
            DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
            DomainEventListener<?> employeeUpdatedElasticEventListener = new EmployeeUpdatedElasticEventListener(employeeRepository, elasticEmployeeRepository);
            DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();
            DomainEventListener<?> employeeDeletedElasticEventListener = new EmployeeDeletedElasticEventListener(elasticEmployeeRepository);
            DomainEventListener<?> employeeIncrementCompanyTotalEmployeesListener = new EmployeeCreateIncrementTotalEmployeesListener(client);
            DomainEventListener<?> employeeDecrementCompanyTotalEmployeesListener = new EmployeeDeletedDecrementTotalEmployeesListener(client);

            List<DomainEventListener<?>> listeners = List.of(
                    companyCreatedLoggingEventListener,
                    companyCreatedElasticEventListener,
                    companyUpdatedLoggingListener,
                    companyUpdatedElasticEventListener,
                    companyDeletedLoggingEventListener,
                    companyDeletedElasticEventListener,
                    employeeCreatedLoggingEventListener,
                    employeeCreatedElasticEventListener,
                    employeeUpdatedLoggingEventListener,
                    employeeUpdatedElasticEventListener,
                    employeeDeletedLoggingEventListener,
                    employeeDeletedElasticEventListener,
                    employeeIncrementCompanyTotalEmployeesListener,
                    employeeDecrementCompanyTotalEmployeesListener
            );

            EventPublisher publisher = new EventPublisher(listeners);

            employeeService = new ElasticEmployeeServiceImpl(employeeRepository, companyRepository, elasticEmployeeRepository, publisher);
            companyService = new ElasticCompanyServiceImpl(companyRepository, elasticCompanyRepository, publisher);
            employeeQueryService = new ElasticEmployeeQueryServiceImpl((ElasticEmployeeQueryRepository) employeeQueryRepository);
            companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

            System.out.println("Running SQL with ELASTIC");
        }

        else {
            ConnectionFactory connectionFactory = DbConnector.getPool();

            employeeRepository = new DBEmployeeRepository(connectionFactory);
            employeeQueryRepository = new DBEmployeeQueryRepository(connectionFactory);
            companyRepository = new DBCompanyRepository(connectionFactory);
            companyQueryRepository = new DBCompanyQueryRepository(connectionFactory);

            employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository);
            companyService = new CompanyServiceImpl(companyRepository);
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