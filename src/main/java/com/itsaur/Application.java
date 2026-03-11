package com.itsaur;

import com.itsaur.dto.*;
import com.itsaur.handler.CompanyHandler;
import com.itsaur.handler.EmployeeHandler;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.model.Employee;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.*;
import com.itsaur.serializer.*;
import com.itsaur.service.*;
import com.itsaur.util.DbConnector;
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

        if (storageType.equalsIgnoreCase("memory")) {

            InMemoryStorage storage = new InMemoryStorage();
            InMemoryEmployeeSearch employeeSearch = new InMemoryEmployeeSearch(storage);
            InMemoryCompanySearch companySearch = new InMemoryCompanySearch(storage);
            employeeRepository = new InMemoryEmployeeRepository(storage);
            employeeQueryRepository = new InMemoryEmployeeQueryRepository(storage, employeeSearch);
            companyRepository = new InMemoryCompanyRepository(storage);
            companyQueryRepository = new InMemoryCompanyQueryRepository(storage, companySearch);

            System.out.println("Running IN-MEMORY");

        } else {

            ConnectionFactory connectionFactory = DbConnector.getPool();

            employeeRepository = new DBEmployeeRepository(connectionFactory);
            employeeQueryRepository = new DBEmployeeQueryRepository(connectionFactory);
            companyRepository = new DBCompanyRepository(connectionFactory);
            companyQueryRepository = new DBCompanyQueryRepository(connectionFactory);
            System.out.println("Running with SQL storage");
        }

        // Services
        EmployeeService employeeService = new EmployeeServiceImpl(employeeRepository,  companyRepository);
        CompanyService companyService = new CompanyServiceImpl(companyRepository);
        EmployeeQueryService employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
        CompanyQueryService companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);

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