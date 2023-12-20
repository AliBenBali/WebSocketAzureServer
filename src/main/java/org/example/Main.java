package org.example;

import azure.MessageProcessor;
import azure.Recipient;
import azure.ServiceBus;
import azure.Topic;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import utils.Queries;

@WebSocket
public class Main {

    public static void main(String[] args) {
        MessageProcessor.startProcessing();
        startWebsocketPingThread();
        Server server = new Server(8080);

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

    private static void startWebsocketPingThread() {
        Thread t = new Thread(() -> {
            while (true) {
                MessageProcessor.pingWebsocket();
                try {
                    Thread.sleep(20_000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.setDaemon(true);
        t.setName("Ping");
        t.start();
    }


    @OnWebSocketConnect
    public void onConnect(Session session) {
        System.out.println("WebSocket connected: " + session.getRemoteAddress().getAddress());
    }

    @OnWebSocketMessage
    public void onMessage(Session session, String message) {
        final ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("Received message: " + message);
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            if (message.contains("DIONE")) {
                MessageProcessor.setWebSocketSession(Recipient.DIONE, session);
                System.out.println("TO: DIONE");
                if (jsonNode.get("Layout") != null) {
                    System.out.println("LayoutQueryDione");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, Queries.getLayoutQueryDione(), Recipient.DIONE);
                }
            } else if (message.contains("RHEA")) {
                MessageProcessor.setWebSocketSession(Recipient.RHEA, session);
                System.out.println("TO: RHEA");
                if (jsonNode.get("MA") != null) {
                    System.out.println("MAQueryRhea");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, Queries.getMaQuery(jsonNode.get("MA").get("id").asText()), Recipient.RHEA);
                }
                if (jsonNode.get("SchalungID") != null) {
                    System.out.println("SchalungIDQueryRhea");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, Queries.getSchalungIdQueryRhea(jsonNode.get("SchalungID").get("id").asText()), Recipient.RHEA);
                }
                if (jsonNode.get("allMachines") != null) {
                    System.out.println("AllMachinesQueryRhea");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, Queries.getAllMachinesQueryRhea(), Recipient.RHEA);
                }
                if (jsonNode.get("SchalungSicht") != null) {
                    System.out.println("SchalungsSichtQueryRhea");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, Queries.getSchalungsSichtQueryRhea(jsonNode.get("SchalungSicht").get("id").textValue()), Recipient.RHEA);
                }
            }
            System.out.println("TO: RHEA");
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
}