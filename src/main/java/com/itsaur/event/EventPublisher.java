package com.itsaur.event;

import reactor.core.publisher.Mono;

public interface EventPublisher {
    Mono<Void> publish(DomainEvent event);
}