package azure;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import org.eclipse.jetty.websocket.api.Session;
import org.example.Main;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.*;

public class MessageProcessor {
    private static final Map<String, Frontend> FRONTEND_SESSION_MAP = new HashMap<>();
    private static final List<Disposable> SERVICE_BUS_SESSIONS = new ArrayList<>();

    private MessageProcessor() {
    }

    public static void setWebSocketSession(String recipient, String frontendId, Session session) {
        FRONTEND_SESSION_MAP.put(frontendId, new Frontend(recipient, session));
    }

    public static void startProcessing() {
        System.out.println("start processing messages");
        startSessionsForTopic(Topic.GRAPHQL_RESPONSE);
        System.out.println("started");
    }

    private static void startSessionsForTopic(String topic) {
        SERVICE_BUS_SESSIONS.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.DIONE));
        SERVICE_BUS_SESSIONS.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.RHEA));
    }

    public static void sendToWebSocket(String recipient, String frontendId, String message) {
        if(frontendId != null) {
            if(FRONTEND_SESSION_MAP.containsKey(frontendId)) {
                Session session = FRONTEND_SESSION_MAP.get(frontendId).session();
                if(session.isOpen()) {
                    try {
                        session.getRemote().sendString(message);
                        System.out.println("sent to websocket to frontendId " + frontendId + ": " + message);
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                } else {
                    FRONTEND_SESSION_MAP.remove(frontendId);
                }
            }
        } else {
            Iterator<Frontend> iterator = FRONTEND_SESSION_MAP.values().iterator();
            while (iterator.hasNext()) {
                Frontend frontend = iterator.next();
                if(frontend.type().equals(recipient)) {
                    Session session = frontend.session();
                    if(session.isOpen()) {
                        try {
                            session.getRemote().sendString(message);
                            System.out.println("sent to websocket for type " + recipient + ": " + message);
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                        }
                    } else {
                        iterator.remove();
                    }
                }
            }
        }
    }

    public static void pingWebsocket() {
        sendToWebSocket(Recipient.DIONE, null, "Ping");
        sendToWebSocket(Recipient.RHEA, null, "Ping");
    }

    public static void processGraphqlResponse(ServiceBusReceivedMessage msg) {
        logSingle(Topic.GRAPHQL_RESPONSE, msg);
        try {
            System.out.println("processing graphql response");
            String body = msg.getBody().toString();
            sendToWebSocket(msg.getTo(), msg.getSubject(), body);
        } catch (Exception e) {
            System.out.println(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static void logSingle(String topic, ServiceBusReceivedMessage message) {
        System.out.println("processing 1 message from topic: " + topic);
        if (Settings.ENABLE_MESSAGE_PROCESSOR_EXTENDED_LOGGING)
            System.out.println("body: " + message.getBody());
    }

    public record Frontend(String type, Session session) {
    }
}
