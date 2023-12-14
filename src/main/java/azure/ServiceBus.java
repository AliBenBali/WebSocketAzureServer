package azure;

import com.azure.messaging.servicebus.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ServiceBus {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBus.class);
    private static final String CONNECTION_STRING = "Endpoint=sb://smart-factory-servicebus-dev-xn-cicek.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Tq64j/jDE3t3dXr5ECisWtAcSDuPeSQrg+ASbCPINPQ=";
    private static final String REPLY_TO = Recipient.DIONE;
    private static final String REPLY_TO_RHEA = Recipient.DIONE;
    private static final int PROCESSOR_TIMEOUT = 2;


    private ServiceBus() {
    }

    public static void sendMessageToTopic(String topic, String recipient, String message, String replyTo) {
        sendMessagesToTopic(topic, recipient, List.of(message), replyTo);
    }

    public static void sendMessagesToTopic(String topic, String recipient, List<String> messages, String replyTo) {
        try (ServiceBusSenderClient senderClient = new ServiceBusClientBuilder().connectionString(CONNECTION_STRING).sender().topicName(topic).buildClient()) {
            sendMessages(senderClient, recipient, messages, topic, replyTo);
        }
    }

    private static void sendMessages(ServiceBusSenderClient senderClient, String recipient, List<String> messages, String topic, String replyTo) {
        if (recipient != null && !recipient.isBlank()) {
            ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();
            List<ServiceBusMessage> serviceBusMessages = new ArrayList<>(messages.size());
            for (String msg : messages) {
                ServiceBusMessage serviceBusMessage = new ServiceBusMessage(msg);
                serviceBusMessage.setReplyTo(replyTo);
                serviceBusMessage.setTo(recipient);
                serviceBusMessage.setSessionId("12345");
                serviceBusMessages.add(serviceBusMessage);
            }
            for (ServiceBusMessage message : serviceBusMessages) {
                if (messageBatch.tryAddMessage(message)) {
                    continue;
                }
                senderClient.sendMessages(messageBatch);
                LOGGER.info("sent " + messageBatch.getCount() + " message(s) to " + topic + " " + recipient);
                messageBatch = senderClient.createMessageBatch();
                if (!messageBatch.tryAddMessage(message)) {
                    LOGGER.error("Message is too large for an empty batch. Skipping. Max size: " + messageBatch.getMaxSizeInBytes());
                }
            }
            if (messageBatch.getCount() > 0) {
                senderClient.sendMessages(messageBatch);
                LOGGER.info("sent " + messageBatch.getCount() + " message(s) to " + topic + " " + recipient);
            }
        } else {
            LOGGER.error("Can't send message(s). No recipient specified.");
        }
    }

    public static Disposable startAsyncMessageProcessor(String topicName, String recipient) {
        ServiceBusSessionReceiverAsyncClient sessionReceiver = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING)
                .sessionReceiver()
                .disableAutoComplete()
                .topicName(topicName)
                .subscriptionName(recipient)
                .buildAsyncClient();
        Mono<ServiceBusReceiverAsyncClient> receiverMono = sessionReceiver.acceptNextSession();
        Flux<Void> sessionMessages = Flux.usingWhen(receiverMono,
                receiver -> receiver.receiveMessages().flatMap(message -> {
                    if (topicName == null) {
                        LOGGER.error("unknown topic: " + topicName);
                        System.out.println("5unknown topic: " + topicName);
                    } else {
                        switch (topicName) {
                            case Topic.GRAPHQL_RESPONSE -> MessageProcessor.processGraphqlResponse(message);
                            case Topic.CUSTOM_MESSAGE -> MessageProcessor.processCustomMessage(message);
                        }
                    }
                    return receiver.complete(message);
                }),
                receiver -> Mono.fromRunnable(() -> {
                    receiver.close();
                    sessionReceiver.close();
                }));
        return sessionMessages.subscribe(unused -> {
            System.out.println("8Gescheiterter Versuch");
        }, error -> System.out.println("9Error: " + error.getMessage()), () -> System.out.println("10Completed"));
    }

    public static List<ServiceBusReceivedMessage> receiveMessagesFromSubscription(String topicName, String subscriptionName) {
        CountDownLatch countdownLatch = new CountDownLatch(1);
        List<ServiceBusReceivedMessage> messageBuffer = new ArrayList<>();
        try (ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder().connectionString(CONNECTION_STRING).processor().topicName(topicName).subscriptionName(subscriptionName).processMessage(context -> messageBuffer.add(processMessage(context))).processError(context -> processError(context, countdownLatch)).buildProcessorClient()) {
            startAndStopProcessor(processorClient);
        }
        return messageBuffer;
    }

    private static void startAndStopProcessor(ServiceBusProcessorClient processorClient) {
        processorClient.start();
        try {
            TimeUnit.SECONDS.sleep(PROCESSOR_TIMEOUT);
        } catch (InterruptedException e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private static ServiceBusReceivedMessage processMessage(ServiceBusReceivedMessageContext context) {
        return context.getMessage();
    }

    private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
        LOGGER.warn("Error when receiving messages from namespace: " + context.getFullyQualifiedNamespace() + ". Entity: " + context.getEntityPath());
        if (!(context.getException() instanceof ServiceBusException exception)) {
            LOGGER.warn("Non-ServiceBusException occurred: " + context.getException().getClass() + ": " + context.getException().getMessage());
            return;
        }
        ServiceBusFailureReason reason = exception.getReason();
        if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND || reason == ServiceBusFailureReason.UNAUTHORIZED) {
            LOGGER.warn("An unrecoverable error occurred. Stopping processing with reason " + reason + ": " + exception.getMessage());
            countdownLatch.countDown();
        } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
            LOGGER.warn("Message lock lost for message: " + context.getException());
        } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        } else {
            LOGGER.warn("Error source " + context.getErrorSource() + ", reason " + reason + ", message: " + context.getException());
        }
    }
}

