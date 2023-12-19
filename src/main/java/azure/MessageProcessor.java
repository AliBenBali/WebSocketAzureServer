package azure;

import Utils.JsonUtils;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import java.util.*;

import org.eclipse.jetty.websocket.api.Session;

public class MessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private static final List<Disposable> sessionList = new ArrayList<>();
    private static boolean extendedLogging = Settings.ENABLE_MESSAGE_PROCESSOR_EXTENDED_LOGGING;

    private static Session webSocketSession;

    public static void setWebSocketSession(Session session) {
        webSocketSession = session;
    }

    private MessageProcessor() {
    }

    public static void startProcessing() {
        LOGGER.info("start processing messages");
        startSessionForTopic(Topic.GRAPHQL_RESPONSE);
        LOGGER.info("started");
    }

    public static void stopProcessing() {
        System.out.println("stop processing messages");
        LOGGER.info("stop processing messages");
        Iterator<Disposable> iterator = sessionList.iterator();
        while (iterator.hasNext()) {
            Disposable entry = iterator.next();
            if (!entry.isDisposed()) {
                entry.dispose();
            }
            iterator.remove();
        }
        LOGGER.info("stopped");
    }

    public static void toggleProcessing() {
        if (sessionList.isEmpty()) {
            startProcessing();
        } else {
            stopProcessing();
        }
    }

    private static void startSessionForTopic(String topic) {
            sessionList.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.DIONE));
            sessionList.add(ServiceBus.startAsyncMessageProcessor(topic, Recipient.RHEA));

    }


    public static void sendToWebSocket(String message) {
        if (webSocketSession != null && webSocketSession.isOpen()) {
            try {
                webSocketSession.getRemote().sendString(message);
                System.out.println("sent to websocket: " + message);
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
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }


    private static void logSingle(String topic, ServiceBusReceivedMessage message) {
        System.out.println("processing 1 message from topic: " + topic);
        LOGGER.info("processing 1 message from topic: " + topic);
        if (extendedLogging)
            LOGGER.info("body: " + message.getBody());
    }


    public static boolean isPrintMessages() {
        return extendedLogging;
    }

    public static void setPrintMessages(boolean printMessages) {
        MessageProcessor.extendedLogging = printMessages;
    }

    public static void processCustomMessage(ServiceBusReceivedMessage msg) {
        logSingle(Topic.CUSTOM_MESSAGE, msg);
        try {
            String body = msg.getBody().toString();
            Map<String, Object> data = JsonUtils.convertToMap(body);
            String messageType = (String) data.get("messageType");
            if (messageType.equals("processStateUpdate")) {
                String workplaceGroupId = (String) data.get("workplaceGroupId");
                int maId = (int) data.get("maId");
                long timeStamp = (long) data.get("timeStamp");
                boolean completed = (boolean) data.get("completed");
             //   Planner.processStateUpdate(workplaceGroupId, maId, timeStamp, completed);
            }
        } catch (Exception e) {
            LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
