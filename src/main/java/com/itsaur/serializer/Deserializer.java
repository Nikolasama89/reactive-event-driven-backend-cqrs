package com.itsaur.serializer;

import reactor.netty.http.server.HttpServerResponse;

public interface Deserializer<T> {
    T deserialize(byte[] bytes);
    default HttpServerResponse contentType(HttpServerResponse response) {
        return response.header("Content-Type", "application/json");
    }
}
