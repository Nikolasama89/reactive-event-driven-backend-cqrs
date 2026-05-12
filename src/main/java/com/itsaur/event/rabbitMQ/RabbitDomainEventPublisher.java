package com.itsaur.event.rabbitMQ;

import com.itsaur.event.DomainEvent;
import com.itsaur.event.EventPublisher;
import com.itsaur.serializer.Serializer;
import com.rabbitmq.client.Channel;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;


public class RabbitDomainEventPublisher implements EventPublisher {
    private final Channel channel;
    private final String exchangeName;
    private final Serializer<DomainEvent> serializer;

    public RabbitDomainEventPublisher(Channel channel, String exchangeName, Serializer<DomainEvent> serializer) {
        this.channel = channel;
        this.exchangeName = exchangeName;
        this.serializer = serializer;

    }

    @Override
    public Mono<Void> publish(DomainEvent event) {
        return Mono.fromCallable(() -> {
                    String routingKey = event.getClass().getSimpleName();
                    byte[] message = serializer.serialize(event);

                    channel.basicPublish(exchangeName, routingKey, null, message);
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }
}