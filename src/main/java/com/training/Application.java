package com.training;

import com.training.config.AppConfig;
import com.training.config.AppContext;
import com.training.config.AppEnvInitializer;
import com.training.config.AppInitializerFactory;
import com.training.handler.CompanyHandler;
import com.training.handler.EmployeeHandler;
import com.training.handler.HttpHandlersFactory;
import com.training.util.DbConnector;
import com.training.util.ElasticSearchConfig;
import com.training.util.RabbitMQConfig;
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