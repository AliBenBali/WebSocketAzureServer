package azure;

import com.azure.messaging.servicebus.*;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

public class ServiceBus {
    private static final String CONNECTION_STRING = "Endpoint=sb://smart-factory-servicebus-dev-xn-cicek.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Tq64j/jDE3t3dXr5ECisWtAcSDuPeSQrg+ASbCPINPQ=";

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
                System.out.println("sent " + messageBatch.getCount() + " message(s) to " + topic + " " + recipient);
                messageBatch = senderClient.createMessageBatch();
                if (!messageBatch.tryAddMessage(message)) {
                    System.out.println("Message is too large for an empty batch. Skipping. Max size: " + messageBatch.getMaxSizeInBytes());
                }
            }
            if (messageBatch.getCount() > 0) {
                senderClient.sendMessages(messageBatch);
                System.out.println("sent " + messageBatch.getCount() + " message(s) to " + topic + " " + recipient);
            }
        } else {
            System.out.println("Can't send message(s). No recipient specified.");
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
                    MessageProcessor.processGraphqlResponse(message);
                    return receiver.complete(message);
                }),
                receiver -> Mono.fromRunnable(() -> {
                    receiver.close();
                    sessionReceiver.close();
                }));
        return sessionMessages.subscribe(unused -> {
        }, error -> System.out.println("Error: " + error.getMessage()), () -> System.out.println("Completed"));
    }
}
