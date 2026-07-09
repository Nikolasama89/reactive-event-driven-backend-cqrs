package com.training.serializer;

import reactor.netty.http.server.HttpServerResponse;

public interface Serializer<T> {
    byte[] serialize(T object);
    default HttpServerResponse contentType(HttpServerResponse response) {
        return response.header("Content-Type", "application/json");
    }
}
