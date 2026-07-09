package com.training.event;

import reactor.core.publisher.Mono;

public interface DomainEventListener<T> {
    Class<T> eventType();
    Mono<Void> handle(T event);
}
