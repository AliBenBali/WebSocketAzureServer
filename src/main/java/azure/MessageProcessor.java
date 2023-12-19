package azure;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import org.eclipse.jetty.websocket.api.Session;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;

public class MessageProcessor {
    private static final List<Disposable> sessionList = new ArrayList<>();

    private static Session webSocketSession;

    private MessageProcessor() {
    }

    public static void setWebSocketSession(Session session) {
        webSocketSession = session;
    }

    public static void startProcessing() {
        System.out.println("start processing messages");
        startSessionsForTopic(Topic.GRAPHQL_RESPONSE);
        System.out.println("started");
    }

    private static void startSessionsForTopic(String topic) {
        sessionList.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.DIONE));
        sessionList.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.RHEA));
    }


    public static void sendToWebSocket(String message) {
        if (webSocketSession != null && webSocketSession.isOpen()) {
            try {
                webSocketSession.getRemote().sendString(message);
                if (Settings.ENABLE_MESSAGE_PROCESSOR_EXTENDED_LOGGING) {
                    System.out.println("sent to websocket: " + message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void processGraphqlResponse(ServiceBusReceivedMessage msg) {
        logSingle(Topic.GRAPHQL_RESPONSE, msg);
        try {
            System.out.println("processing graphql response");
            String body = msg.getBody().toString();
            sendToWebSocket(body);
        } catch (Exception e) {
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }


    private static void logSingle(String topic, ServiceBusReceivedMessage message) {
        System.out.println("processing 1 message from topic: " + topic);
        if (Settings.ENABLE_MESSAGE_PROCESSOR_EXTENDED_LOGGING)
            System.out.println("body: " + message.getBody());
    }
}
