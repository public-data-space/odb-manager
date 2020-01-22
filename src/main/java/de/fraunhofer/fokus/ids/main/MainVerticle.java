package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.ConnectorAvailableMessage;
import de.fraunhofer.iais.eis.ConnectorUnavailableMessage;
import de.fraunhofer.iais.eis.ConnectorUpdateMessage;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MainVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
    private Router router;

    @Override
    public void start (Future<Void> startFuture){
        this.router = Router.router(vertx);
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setWorker(true);

        Future<String> deployment = Future.succeededFuture();
        deployment.setHandler(ar -> {
            if (ar.succeeded()) {
                router = Router.router(vertx);
                createHttpServer(vertx);
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });



    }

    private void createHttpServer(Vertx vertx) {
        HttpServer server = vertx.createHttpServer();

        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("Access-Control-Allow-Credentials");
        allowedHeaders.add("origin");
        allowedHeaders.add("authorization");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");
        allowedHeaders.add("Access-Control-Allow-Headers");
        allowedHeaders.add("Access-Control-Allow-Methods");
        allowedHeaders.add("X-PINGARUNER");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);

        router.route().handler(CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
        router.route().handler(BodyHandler.create());

        router.post("/data").handler(this::getData);
        LOGGER.info("Starting odb manager ");
        server.requestHandler(router).listen(8080);
        LOGGER.info("odb-manager deployed on port "+8080);
    }

    private void getData (RoutingContext routingContext){
            String data = routingContext.getBodyAsString();
            String header = getHeader(data);
            String body = getBody(data);
            try {
              ConnectorAvailableMessage connectorAvailableMessage =  Json.decodeValue(header, ConnectorAvailableMessage.class);
              register(Json.decodeValue(body, Connector.class));
            } catch (DecodeException exception) {
                try {
                    ConnectorUnavailableMessage connectorUnavailableMessage =  Json.decodeValue(header, ConnectorUnavailableMessage.class);
                    unregister(Json.decodeValue(body, Connector.class));
                } catch (DecodeException exception2) {
                    try {
                        ConnectorUpdateMessage connectorUpdateMessage =  Json.decodeValue(header, ConnectorUpdateMessage.class);
                        update(Json.decodeValue(body, Connector.class));
                    }
                    catch (DecodeException exception3){
                        routingContext.fail(exception3);
                    }
                }
            }
    }

    private void update(Connector body) {
        System.out.println("Hier ist update");
    }

    private void register(Connector body) {
        System.out.println("Hier ist register");
    }

    private void unregister(Connector body) {
        System.out.println("Hier ist unregister");
    }

    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }
    private String getHeader (String input){
        JsonObject header = new JsonObject();
        String stringOfHeader  = input.substring(input.indexOf("{"),input.indexOf("}")+1);
        header.put("Header ",stringOfHeader);
        return stringOfHeader;
    }
    private String getBody (String input){
        JsonObject body = new JsonObject();
        String stringOfBody  = input.substring(input.indexOf("{", input.indexOf("{") + 1),input.lastIndexOf("}")+1);
        body.put("Body ",stringOfBody);
        return stringOfBody;
    }


}
