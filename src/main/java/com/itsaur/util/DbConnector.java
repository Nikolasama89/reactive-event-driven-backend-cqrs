package com.itsaur.util;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;


import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

public class DbConnector {

    private DbConnector() {}

    private static final ConnectionPool POOL = connectionPool();

    public static ConnectionPool connectionPool() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
                .option(DRIVER, "postgresql")
                .option(HOST, "127.0.0.1")
                .option(PORT, 5433)
                .option(USER, "postgres")
                .option(PASSWORD, "postgres")
                .option(DATABASE, "employeesdb")
                .build();

        ConnectionFactory cf = ConnectionFactories.get(options);

        ConnectionPoolConfiguration poolConfig = ConnectionPoolConfiguration.builder(cf)
                .maxSize(20)                         // max simultaneous connections
                .maxIdleTime(Duration.ofMinutes(30)) // close idle connections after 30m
                .validationQuery("SELECT 1")         // check connection is alive
                .build();

        return new ConnectionPool(poolConfig);
    }

    public static ConnectionPool getPool(){
        return POOL;
    }
}