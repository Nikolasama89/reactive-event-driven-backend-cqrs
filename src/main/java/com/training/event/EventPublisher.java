package com.training.event;

import reactor.core.publisher.Mono;

public interface EventPublisher {
    Mono<Void> publish(DomainEvent event);
}