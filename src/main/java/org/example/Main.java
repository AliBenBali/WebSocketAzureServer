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

import java.util.concurrent.ForkJoinPool;

@WebSocket
public class Main {
    Boolean LoggedIn = false;
    private final static ForkJoinPool pool = new ForkJoinPool(8);
    public static void main(String[] args) {
        org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8080);
        loopMessages();

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

    private static void loopMessages() {
        Thread t = new Thread(() -> {
            MessageProcessor messageProcessor = new MessageProcessor();
            while (true) {
                pool.execute(messageProcessor::processGraphqlResponsesRhea);
                pool.execute(messageProcessor::processGraphqlResponsesDione);

                try {
                    Thread.sleep(4000);
                    System.out.println("Looping...");




                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t.setDaemon(true);
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
        ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                "  factoryWorkerByMaId(maId: 101) {\n" +
                "    maId\n" +
                "    firstName\n" +
                "    lastName\n" +
                "  }\n" +
                "}");
        System.out.println("Message sent to Titan");
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            if (jsonNode.get("Login") != null){
                if(jsonNode.get("Login").get("user").asText().equals("Dione") &&jsonNode.get("Login").get("pswd").asText().equals("admin") || jsonNode.get("Login").get("user").asText().equals("Rhea") &&jsonNode.get("Login").get("pswd").asText().equals("admin")){
                    LoggedIn = true;
                    System.out.println("Login erfolgreich");
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                            "  factoryWorkerByMaId(maId: 100) {\n" +
                            "    maId\n" +
                            "    firstName\n" +
                            "    lastName\n" +
                            "  }\n" +
                            "}");

                    try {
                        session.getRemote().sendString("LoggedIn");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            if(LoggedIn){
                if (jsonNode.get("GetSchalung") != null){
                    //DB Query
                    if(jsonNode.get("GetSchalung").get("SchalungsID").asText().equals("1")){

                        try {
                            session.getRemote().sendString("{\"Schalung\":{\"SchalungsID\":1,\"SchalungVorbereitenStart\":\"13:57\",\"SchalungVorbereitenStop\":\"14:15\",\"KorbEinsetztenStart\":\"14:16\",\"KorbEinsetztenStop\":\"14:30\"}}");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

        if (message.contains("Schalter")){
            //azure connection
            //---------Azure Zeugs
            //Nachricht zurück

                try {
                    session.getRemote().sendString("Schalter geupdated");
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
        if (message.contains("Fertig")){
            //azure connection
            //---------Azure Zeugs
            //Nachricht zurück
            try {
                session.getRemote().sendString("Toll!");
            } catch (Exception e) {
                e.printStackTrace();
            }
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