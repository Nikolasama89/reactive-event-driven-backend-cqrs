package com.training.util;

import com.training.config.AppConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQConfig {
    private RabbitMQConfig() {}

    private static String host;
    private static String username;
    private static String password;
    private static Connection connection;

    public static void init(AppConfig config) {
        host = config.rabbitHost();
        username = config.rabbitUser();
        password = config.rabbitPassword();
    }

    public static Connection getConnection() throws Exception {
        if (host == null)
            throw new IllegalStateException("RabbitMQConfig.init(config) must be called first");
        if (connection == null || !connection.isOpen()) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setUsername(username);
            factory.setPassword(password);
            connection = factory.newConnection();
        }
        return connection;
    }

    public static Channel getChannel() throws Exception {
        return getConnection().createChannel();
    }
}
