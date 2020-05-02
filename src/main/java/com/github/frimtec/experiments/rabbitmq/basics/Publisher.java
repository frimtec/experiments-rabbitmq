package com.github.frimtec.experiments.rabbitmq.basics;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static com.github.frimtec.experiments.rabbitmq.basics.QueueConfiguration.QUEUE_NAME;
import static com.github.frimtec.experiments.rabbitmq.basics.QueueConfiguration.RABBIT_MQ_SERVER_URL;

public class Publisher {

    private static final String EXCHANGE_NAME = "example-exchange";
    private static final String ROUTING_KEY = "example-routing-key";

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBIT_MQ_SERVER_URL);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .build();
            IntStream.rangeClosed(1, 10).forEach(i -> {
                try {
                    channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, properties, ("Message number " + i).getBytes());
                    logger.info("Published message: {}", i);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot send message", e);
                }
            });
        }
    }
}
