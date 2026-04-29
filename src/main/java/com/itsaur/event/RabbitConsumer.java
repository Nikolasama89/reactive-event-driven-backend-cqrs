package com.itsaur.event;

import com.itsaur.serializer.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class RabbitConsumer {
    private static final Logger log = LoggerFactory.getLogger(RabbitConsumer.class);
    private final Channel channel;
    private final String queueName;
    private final List<DomainEventListener<?>> listeners;
    private final Map<String, Deserializer<? extends DomainEvent>> deserializers;

    public RabbitConsumer(
            Channel channel,
            String queueName,
            List<DomainEventListener<?>> listeners,
            Map<String, Deserializer<? extends DomainEvent>> deserializers,
            String exchangeName,
            String[] routingKeys) {
        this.channel = channel;
        this.queueName = queueName;
        this.listeners = listeners;
        this.deserializers = deserializers;

        try {
            channel.exchangeDeclare(exchangeName, "direct", true);
            channel.queueDeclare(queueName, true, false, false, null);

            for (String key : routingKeys) {
                channel.queueBind(queueName, exchangeName, key);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to declare queue/exchange", e);
        }
    }

    public void startConsuming() {
        DeliverCallback callback = (consumerTag, delivery) -> {
            String routingKey = delivery.getEnvelope().getRoutingKey();
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();

            try {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                if (message.isBlank()) {
                    channel.basicAck(deliveryTag, false);
                    return;
                }

                Deserializer<? extends DomainEvent> deserializer = deserializers.get(routingKey);

                if (deserializer == null) {
                    log.error("No deserializer found for routingKey={}", routingKey);
                    channel.basicNack(deliveryTag, false, false);
                    return;
                }

                DomainEvent event = deserializer.deserialize(delivery.getBody());

                Flux.fromIterable(listeners)
                        .filter(l -> l.eventType().isInstance(event))
                        .concatMap(l -> {
                            @SuppressWarnings("unchecked")
                            DomainEventListener<DomainEvent> listener = (DomainEventListener<DomainEvent>) l;
                            return listener.handle(event)
                                    .doOnError(e -> log.error("Listener {} failed for event {}",
                                            listener.getClass().getSimpleName(), event, e))
                                    .onErrorResume(e -> Mono.empty());
                        })
                        .then()
                        .doOnSuccess(v -> {
                            try {
                                channel.basicAck(deliveryTag, false);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .doOnError(e -> {
                            try {
                                channel.basicNack(deliveryTag, false, false);
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        })
                        .subscribe();
            } catch (Exception e) {
                e.printStackTrace();
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            }
        };

        try {
            channel.basicConsume(queueName, false, callback, consumerTag -> {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
