package com.itsaur.config;

public class AppConfig {
    private final AppMode mode;

    public AppConfig(AppMode mode) {
        this.mode = mode;
    }

    public AppMode getMode(){
        return mode;
    }

    public static AppConfig fromEnv(){
        String modeStr = System.getenv().getOrDefault("APP_MODE", "SQL_RABBIT");
        AppMode mode = AppMode.valueOf(modeStr.toUpperCase().replace("-", "_"));
        return new AppConfig(mode);
    }
}
