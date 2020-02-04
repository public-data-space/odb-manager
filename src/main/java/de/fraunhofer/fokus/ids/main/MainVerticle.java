package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.controller.BrokerMessageController;
import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageServiceVerticle;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
    private Router router;
    private BrokerMessageController brokerMessageController;
    @Override
    public void start (Future<Void> startFuture){
        this.router = Router.router(vertx);
        this.brokerMessageController = new BrokerMessageController(vertx);
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setWorker(true);

        Future<String> deployment = Future.succeededFuture();
        deployment.compose(id2 -> {
            Future<String> brokerMessage = Future.future();
            vertx.deployVerticle(BrokerMessageServiceVerticle.class.getName(), deploymentOptions, brokerMessage.completer());
            return brokerMessage;
        }).setHandler(ar -> {
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

        router.post("/data").handler(routingContext -> brokerMessageController.getData(routingContext.getBodyAsString()));
        LOGGER.info("Starting odb manager ");
        server.requestHandler(router).listen(8080);
        LOGGER.info("odb-manager deployed on port "+8080);
    }




    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }



}
