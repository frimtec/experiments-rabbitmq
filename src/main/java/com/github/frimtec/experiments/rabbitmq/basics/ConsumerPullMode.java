package com.github.frimtec.experiments.rabbitmq.basics;

import com.rabbitmq.client.*;
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

public class ConsumerPullMode {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBIT_MQ_SERVER_URL);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            while (channel.messageCount(QUEUE_NAME) > 0) {
                GetResponse response = channel.basicGet(QUEUE_NAME, true);
                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                logger.info("Message consumed: {}", message);
            }
        }
    }
}
