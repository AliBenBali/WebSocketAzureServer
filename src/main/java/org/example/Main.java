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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

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
            System.out.println("JsonNode: ");
            if (jsonNode.get("MA") != null) {
                System.out.println("MA: " + jsonNode.get("MA").get("id").asText());
                ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                        "  factoryWorkerByMaId(maId:" + jsonNode.get("MA").get("id").asText() + ") {\n" +
                        "    maId\n" +
                        "    firstName\n" +
                        "    lastName\n" +
                        "  }\n" +
                        "}" , Recipient.DIONE);

            }

            if (jsonNode.get("Layout") != null) {
                System.out.println("Layout: ");
                String machineType = "Schalung";
                String graphqlQuery = """
                          query MyQuery {
                          machinesByType(machineType: "Schalung") {
                            name
                            currentWorkplaceGroup { 
                              process{
                                name
                              }
                            }
                          }   
                        }
                                                """;
                try {
                    ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.SATURN, "query MyQuery {\n" +
                            "                          machinesByType(machineType: \"Schalung\") {\n" +
                            "                            name\n" +
                            "                            currentWorkplaceGroup { \n" +
                            "                              process{\n" +
                            "                                name\n" +
                            "                              }\n" +
                            "                            }\n" +
                            "                          }   \n" +
                            "                        }", Recipient.DIONE);
                    System.out.println("GraphQL query for machine type \"" + machineType + "\" sent successfully.");
                } catch (Exception e) {
                    System.err.println("Error sending GraphQL query: " + e.getMessage());
                    // Handle the error as needed
                }


            }
            if (LoggedIn) {
                if (jsonNode.get("GetSchalung") != null) {
                    //DB Query
                    if (jsonNode.get("GetSchalung").get("SchalungsID").asText().equals("1")) {

                        try {
                            session.getRemote().sendString("{\"Schalung\":{\"SchalungsID\":1,\"SchalungVorbereitenStart\":\"13:57\",\"SchalungVorbereitenStop\":\"14:15\",\"KorbEinsetztenStart\":\"14:16\",\"KorbEinsetztenStop\":\"14:30\"}}");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (message.contains("Schalter")) {
            //azure connection
            //---------Azure Zeugs
            //Nachricht zurück

            try {
                session.getRemote().sendString("Schalter geupdated");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (message.contains("MAID")) {
            try {
                ServiceBus.sendMessageToTopic(Topic.GRAPHQL_QUERY, Recipient.TITAN, "query ma {\n" +
                        "  factoryWorkerByMaId(maId: 100) {\n" +
                        "    maId\n" +
                        "    firstName\n" +
                        "    lastName\n" +
                        "  }\n" +
                        "}", Recipient.DIONE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (message.contains("Fertig")) {
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