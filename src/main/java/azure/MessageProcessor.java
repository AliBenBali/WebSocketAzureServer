package azure;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private boolean printMessages;



    public MessageProcessor() {
        this.printMessages = false;
    }

    public void processGraphqlResponsesDione() {
        List<ServiceBusReceivedMessage> messages = ServiceBus.receiveMessagesFromSubscription(Topic.GRAPHQL_RESPONSE, Recipient.DIONE);
        logBatch(Topic.GRAPHQL_RESPONSE, messages);
        for (ServiceBusReceivedMessage msg : messages) {
            logSingle(Topic.GRAPHQL_RESPONSE, msg);
            try {
                String body = msg.getBody().toString();
                LOGGER.info("Message body: " + body);
                System.out.println("Message body: " + body);
            } catch (Exception e) {
                LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    public void processGraphqlResponsesRhea() {
        List<ServiceBusReceivedMessage> messages = ServiceBus.receiveMessagesFromSubscription(Topic.GRAPHQL_RESPONSE, Recipient.RHEA);
        logBatch(Topic.GRAPHQL_RESPONSE, messages);
        for (ServiceBusReceivedMessage msg : messages) {
            logSingle(Topic.GRAPHQL_RESPONSE, msg);
            try {
                String body = msg.getBody().toString();
                LOGGER.info("Message body: " + body);
                System.out.println("Message body: " + body);
            } catch (Exception e) {
                LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
    }

    private void logBatch(String topic, List<ServiceBusReceivedMessage> messages) {
        if (!messages.isEmpty())
            LOGGER.info("processing " + messages.size() + " message(s) from topic: " + topic);
    }

    private void logSingle(String topic, ServiceBusReceivedMessage message) {
        if (printMessages)
            LOGGER.info("Topic: " + topic + ", Sender: " + message.getReplyTo() + ", Body: " + message.getBody());
    }
}
