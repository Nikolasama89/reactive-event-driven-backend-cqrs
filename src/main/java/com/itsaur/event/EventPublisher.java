package com.itsaur.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class EventPublisher {

    private final List<DomainEventListener<?>> listeners;

    public EventPublisher(List<DomainEventListener<?>> listeners) {
        this.listeners = listeners;
    }

    public Mono<Void> publish(Object event) {
        return Flux.fromIterable(listeners)
                .filter(listener -> listener.eventType().isInstance(event))
                .flatMap(listener -> invoke(listener, event))
                .then();
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<Void> invoke(DomainEventListener<?> listener, Object event) {
        DomainEventListener<T> l = (DomainEventListener<T>) listener;
        return l.handle((T) event);
    }
}
