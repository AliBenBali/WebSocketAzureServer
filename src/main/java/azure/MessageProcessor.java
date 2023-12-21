package azure;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import org.eclipse.jetty.websocket.api.Session;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MessageProcessor {
    private static final Map<String, Frontend> FRONTEND_SESSION_MAP = new HashMap<>();
    private static final Map<String, Disposable> sessionMap = new HashMap<>();

    private MessageProcessor() {
    }

    public static void setWebSocketSession(String recipient, String frontendId, Session session) {
        FRONTEND_SESSION_MAP.put(frontendId, new Frontend(recipient, session));
    }

    public static void startProcessing() {
        System.out.println("start processing messages");
        startSessionsForRecipient(Recipient.DIONE);
        startSessionsForRecipient(Recipient.RHEA);
        System.out.println("started");
    }

    public static void toggleRheaProcessing() {
        if (sessionMap.containsKey(Recipient.RHEA)) {
            Disposable disposable = sessionMap.get(Recipient.RHEA);
            if (!disposable.isDisposed()) {
                disposable.dispose();
            }
            sessionMap.remove(Recipient.RHEA);
            System.out.println("ServiceBus RHEA closed");
        } else {
            startSessionsForRecipient(Recipient.RHEA);
        }
    }

    private static void startSessionsForRecipient(String recipient) {
        if (!sessionMap.containsKey(recipient)) {
            sessionMap.put(recipient, ServiceBus.startAsyncMessageProcessor(Topic.GRAPHQL_RESPONSE, recipient));
            System.out.println("ServiceBus " + recipient + " started");
        }
    }

    public static void sendToWebSocket(String recipient, String frontendId, String message) {
        if (frontendId != null) {
            if (FRONTEND_SESSION_MAP.containsKey(frontendId)) {
                Session session = FRONTEND_SESSION_MAP.get(frontendId).session();
                if (session.isOpen()) {
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
                if (frontend.type().equals(recipient)) {
                    Session session = frontend.session();
                    if (session.isOpen()) {
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
