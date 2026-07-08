package com.itsaur.util;

import com.itsaur.config.AppConfig;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;


import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class DbConnector {
    private DbConnector() {}

    private static ConnectionPool pool;

    public static void init(AppConfig config) {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, config.dbHost())
                .option(PORT, config.dbPort())
                .option(USER, config.dbUser())
                .option(PASSWORD, config.dbPassword())
                .option(DATABASE, config.dbName())
                .build();

        ConnectionFactory cf = ConnectionFactories.get(options);
        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(cf)
                .maxSize(20)
                .maxIdleTime(Duration.ofMinutes(30))
                .validationQuery("SELECT 1")
                .build();

        pool = new ConnectionPool(poolConfig);
    }

    public static ConnectionPool getPool() {
        if (pool == null)
            throw new IllegalStateException("DbConnector.init(config) must be called first");
        return pool;
    }
}