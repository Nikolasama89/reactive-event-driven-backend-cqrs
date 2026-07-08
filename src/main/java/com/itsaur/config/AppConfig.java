package com.itsaur.config;

public class AppConfig {
    private final AppMode mode;
    private final String dbHost;
    private final int dbPort;
    private final String dbName;
    private final String dbUser;
    private final String dbPassword;
    private final String esHost;
    private final int esPort;
    private final String kafkaBootstrap;
    private final String rabbitHost;
    private final String rabbitUser;
    private final String rabbitPassword;

    public AppConfig(AppMode mode,
                     String dbHost, int dbPort, String dbName, String dbUser, String dbPassword,
                     String esHost, int esPort,
                     String kafkaBootstrap,
                     String rabbitHost, String rabbitUser, String rabbitPassword) {
        this.mode = mode;
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.esHost = esHost;
        this.esPort = esPort;
        this.kafkaBootstrap = kafkaBootstrap;
        this.rabbitHost = rabbitHost;
        this.rabbitUser = rabbitUser;
        this.rabbitPassword = rabbitPassword;
    }

    public static AppConfig fromEnv() {
        return new AppConfig(
                requiredMode("APP_MODE"),
                required("DB_HOST"), requiredInt("DB_PORT"), required("DB_NAME"),
                required("DB_USER"), required("DB_PASSWORD"),
                required("ES_HOST"), requiredInt("ES_PORT"),
                required("KAFKA_BOOTSTRAP"),
                required("RABBIT_HOST"), required("RABBIT_USER"), required("RABBIT_PASSWORD")
        );
    }

    public AppMode getMode(){
        return mode;}
    public String dbHost(){return dbHost; }
    public int dbPort(){return dbPort; }
    public String dbName(){return dbName; }
    public String dbUser(){return dbUser; }
    public String dbPassword(){return dbPassword; }
    public String esHost(){return esHost; }
    public int esPort(){return esPort; }
    public String kafkaBootstrap(){return kafkaBootstrap; }
    public String rabbitHost(){return rabbitHost; }
    public String rabbitUser(){return rabbitUser; }
    public String rabbitPassword(){return rabbitPassword; }

    private static String required(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank())
            throw new IllegalStateException("Missing env var: " + key);
        return v;
    }

    private static int requiredInt(String key) {
        return Integer.parseInt(required(key));
    }

    private static AppMode requiredMode(String key) {
        return AppMode.valueOf(required(key).trim().toUpperCase());
    }
}
