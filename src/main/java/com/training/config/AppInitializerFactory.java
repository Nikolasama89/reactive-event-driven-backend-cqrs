package com.training.config;

public class AppInitializerFactory {
    public static AppEnvInitializer getInitializer(AppMode mode){
        return switch (mode){
            case SQL_RABBIT -> new SqlRabbitInitializer();
            case SQL_KAFKA -> new SqlKafkaInitializer();
            case SQL_RABBIT_ELASTIC -> new SqlRabbitElastic();
            case SQL_KAFKA_ELASTIC -> new SqlKafkaElasticInitializer();
            case MEMORY_RABBIT -> new InMemoryRabbitInitializer();
            case MEMORY_KAFKA -> new InMemoryKafkaInitializer();
            case STREAM_KAFKA_ELASTIC -> new KafkaStreamsElasticInitializer();
        };
    }
}
