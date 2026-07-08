package com.itsaur;

import com.itsaur.config.AppConfig;
import com.itsaur.config.AppContext;
import com.itsaur.config.AppEnvInitializer;
import com.itsaur.config.AppInitializerFactory;
import com.itsaur.handler.CompanyHandler;
import com.itsaur.handler.EmployeeHandler;
import com.itsaur.handler.HttpHandlersFactory;
import com.itsaur.util.DbConnector;
import com.itsaur.util.ElasticSearchConfig;
import com.itsaur.util.RabbitMQConfig;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;


public class Application {

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv();
        DbConnector.init(config);
        ElasticSearchConfig.init(config);
        RabbitMQConfig.init(config);
        AppEnvInitializer initializer = AppInitializerFactory.getInitializer(config.getMode());
        AppContext context = initializer.initialize();

        // Handlers
        EmployeeHandler employeeHandler =
                HttpHandlersFactory.createEmployeeHandler(context.getEmployeeService(), context.getEmployeeQueryService());
        CompanyHandler companyHandler =
                HttpHandlersFactory.createCompanyHandler(context.getCompanyService(), context.getCompanyQueryService());

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