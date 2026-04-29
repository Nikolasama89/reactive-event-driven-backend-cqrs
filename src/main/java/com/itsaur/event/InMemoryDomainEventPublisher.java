package com.itsaur.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class InMemoryDomainEventPublisher implements EventPublisher {

    private final List<DomainEventListener<?>> listeners;

    public InMemoryDomainEventPublisher(List<DomainEventListener<?>> listeners) {
        this.listeners = listeners;
    }

    public Mono<Void> publish(DomainEvent event) {
        return Flux.fromIterable(listeners)
                .filter(listener -> listener.eventType().isInstance(event))
                .concatMap(listener -> invoke(listener, event))
                .then();
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<Void> invoke(DomainEventListener<?> listener, DomainEvent event) {
        DomainEventListener<T> l = (DomainEventListener<T>) listener;
        return l.handle((T) event);
    }
}
