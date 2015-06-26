package com.spann;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;

public class Subscriber implements MessageListener {

    private static final String BROKER_URL = "tcp://localhost:61616";

    private static final Boolean NON_TRANSACTED = false;

    public static void main(String[] args) {
        String url = BROKER_URL;
        if (args.length > 0) {
            url = args[0].trim();
        }
        System.out.println("\nWaiting to receive messages... Either waiting for END message or press Ctrl+C to exit");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin","password",url);
        Connection connection = null;
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            connection = connectionFactory.createConnection();
            String clientId = "sunil";
            connection.setClientID(clientId);

            connection.start();

            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic(">");

            MessageConsumer consumer = session.createDurableSubscriber(destination, clientId);

            consumer.setMessageListener(new Subscriber());

            latch.await();
            consumer.close();
            session.close();
        } catch (Exception e) {
            System.out.println("Caught exception!");
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    System.out.println("Could not close an open connection...");
                }
            }
        }
    }

    public void onMessage(Message message) {
        try {
            System.out.println(((ActiveMQDestination) message.getJMSDestination()).getPhysicalName());

            if (message instanceof TextMessage) {
                String text = ((TextMessage) message).getText();
                System.out.println("Received :" + text);
            }
        } catch (JMSException e) {
            System.out.println("Got a JMS Exception!");
        }
    }
}
