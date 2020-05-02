package com.github.frimtec.experiments.rabbitmq.basics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import static com.github.frimtec.experiments.rabbitmq.basics.QueueConfiguration.QUEUE_NAME;
import static com.github.frimtec.experiments.rabbitmq.basics.QueueConfiguration.RABBIT_MQ_SERVER_URL;

public class ConsumerPushMode {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBIT_MQ_SERVER_URL);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicConsume(QUEUE_NAME, true, (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                logger.info("Message consumed: {}", message);
            }, (consumerTag, sig) -> logger.info("Cancel received"));

            logger.info("<press any key to terminate>");
            System.in.read();
        }
    }
}
