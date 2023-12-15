package org.example;

import azure.MessageProcessor;
import azure.Recipient;
import azure.ServiceBus;
import azure.Topic;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.server.WebSocketServerFactory;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;

@WebSocket
public class Main {
    Boolean LoggedIn = false;
    private final static ForkJoinPool pool = new ForkJoinPool(8);

    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private static String lastProcessedMessageToDione = "empty";
    private static String lastProcessedMessageToRhea = "empty";


    public static void main(String[] args) {
        MessageProcessor.startProcessing();
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8080);


        WebSocketHandler wsHandler = new WebSocketHandler() {
            @Override
            public void configure(WebSocketServletFactory factory) {
                factory.register(Main.class);
            }
        };

        server.setHandler(wsHandler);

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @OnWebSocketConnect
    public void onConnect(Session session) {
        System.out.println("WebSocket connected: " + session.getRemoteAddress().getAddress());
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) throws IOException {
        MessageProcessor.setWebSocketSession(session);
        final ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("Received message: " + message);
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            if (message.contains("DIONE")) {
                System.out.println("TO: DIONE");
                if (jsonNode.get("MA") != null) {
                    System.out.println("MAQueryDione");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                            "  factoryWorkerByMaId(maId:" + jsonNode.get("MA").get("id").asText() + ") {\n" +
                            "    maId\n" +
                            "    firstName\n" +
                            "    lastName\n" +
                            "  }\n" +
                            "}", Recipient.DIONE);

                }

                if (jsonNode.get("Layout") != null) {
                    System.out.println("LayoutQueryDione");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, "query q {\n" +
                            "  machinesByType(machineType: \"Schalung\") {\n" +
                            "    name\n" +
                            "    currentWorkplaceGroup {\n" +
                            "      prodOrder {\n" +
                            "        orderNumber\n" +
                            "        workplaceGroups {\n" +
                            "          process {\n" +
                            "            name\n" +
                            "          }\n" +
                            "          processStates {\n" +
                            "            startTime\n" +
                            "            endTime\n" +
                            "            isCompleted\n" +
                            "          }\n" +
                            "        }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }\n" +
                            "}", Recipient.DIONE);
                }
            } else if (message.contains("RHEA")) {
                if (jsonNode.get("MA") != null) {
                    System.out.println("MAQueryDione");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                            "  factoryWorkerByMaId(maId:" + jsonNode.get("MA").get("id").asText() + ") {\n" +
                            "    maId\n" +
                            "    firstName\n" +
                            "    lastName\n" +
                            "  }\n" +
                            "}", Recipient.RHEA);

                }

                if (jsonNode.get("SchalungID") != null) {
                    System.out.println("MAQueryDione");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, "query schalung{\n" +
                            "  prodOrderByMachineId(id: " + jsonNode.get("SchalungID").get("id") + ") {\n" +
                            "    workplaceGroups {\n" +
                            "      machines {\n" +
                            "        machineType {\n" +
                            "          id\n" +
                            "        }\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }\n" +
                            "}", Recipient.RHEA);

                }
                System.out.println("TO: RHEA");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        System.out.println("WebSocket closed: " + statusCode + " - " + reason);
    }

    @OnWebSocketError
    public void onError(Session session, Throwable error) {
        System.err.println("WebSocket error: " + error.getMessage());
    }

    public String getLastProcessedMessageToDione() {
        return lastProcessedMessageToDione;
    }

    public String getLastProcessedMessageToRhea() {
        return lastProcessedMessageToRhea;
    }

    private static void setLastProcessedMessageToDione(String message) {
        lastProcessedMessageToDione = message;
    }

    private static void setLastProcessedMessageToRhea(String message) {
        lastProcessedMessageToRhea = message;
    }
}