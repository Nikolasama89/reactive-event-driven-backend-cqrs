package com.itsaur.util;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQConfig {
    private static final String HOST = "localhost";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";

    private static Connection connection;

    public static Connection getConnection() throws Exception {
        if (connection == null || !connection.isOpen()) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST);
            factory.setUsername(USERNAME);
            factory.setPassword(PASSWORD);
            connection = factory.newConnection();
        }
        return connection;
    }

    public static Channel getChannel() throws Exception {
        return getConnection().createChannel();
    }

}
