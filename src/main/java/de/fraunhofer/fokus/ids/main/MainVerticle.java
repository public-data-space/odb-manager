package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.controller.*;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.models.IDSMessage;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.BrokerMessageServiceVerticle;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseServiceVerticle;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerServiceVerticle;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.fokus.ids.utils.InitService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.*;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.http.entity.ContentType;

import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
    private Router router;
    private QueryMessageController queryMessageController;
    private IDSService idsService;
    private TSConnector tsConnector;
    private RegisterController registerController;
    private UpdateController updateController;
    private UnregisterController unregisterController;
    @Override
    public void start(Promise<Void> startPromise) {
        this.router = Router.router(vertx);
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setWorker(true);

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        Future<String> deployment = Future.succeededFuture();

        retriever.getConfig(config -> {
            if (config.succeeded()){
                WebClient webClient = WebClient.create(vertx);
                CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                        .retryPolicy(count -> count * 1000L);
                this.tsConnector = TSConnector.create(webClient, breaker, config.result());
                this.queryMessageController = new QueryMessageController(tsConnector, vertx);
                GraphManager graphManager = new GraphManager(vertx, tsConnector);
                this.updateController = new UpdateController(vertx, graphManager,tsConnector);
                this.unregisterController = new UnregisterController(vertx, graphManager,tsConnector);
                this.registerController = new RegisterController(vertx,graphManager,tsConnector);

                deployment.compose(id1 -> {
                    Promise<String> dcatTransformer = Promise.promise();
                    Future<String> dcatTransformerFuture = dcatTransformer.future();
                    vertx.deployVerticle(DCATTransformerServiceVerticle.class.getName(), deploymentOptions, dcatTransformerFuture);
                    return dcatTransformerFuture;
                }).compose(id2 -> {
                    Promise<String> brokerMessage = Promise.promise();
                    Future<String> brokerMessageFuture = brokerMessage.future();
                    vertx.deployVerticle(BrokerMessageServiceVerticle.class.getName(), deploymentOptions, brokerMessageFuture);
                    return brokerMessageFuture;
                }).compose(id3 -> {
                    Promise<String> databaseMessage = Promise.promise();
                    Future<String> databaseMessageFuture = databaseMessage.future();
                    vertx.deployVerticle(DatabaseServiceVerticle.class.getName(), deploymentOptions, databaseMessage);
                    return databaseMessageFuture;
                }).setHandler(ar -> {
                    if (ar.succeeded()) {
                        Future initFuture = Promise.promise().future();
                        new InitService(vertx).initDatabase(initFuture);
                        if (initFuture.succeeded()) {
                            router = Router.router(vertx);
                            createHttpServer(vertx);
                            idsService = new IDSService(vertx,tsConnector);
                            startPromise.complete();
                        } else {
                            startPromise.fail(initFuture.cause());
                        }
                    } else {
                        startPromise.fail(ar.cause());
                    }
                });
            }
            else{
                LOGGER.error(config.cause());
                startPromise.fail(config.cause());
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
        router.post("/data").handler(routingContext -> getData(routingContext.getBodyAsString(),
                reply -> reply(reply, routingContext.response())));
        router.route("/about").handler(routingContext -> about(reply -> reply(reply, routingContext.response())));
        LOGGER.info("Starting odb manager ");
        server.requestHandler(router).listen(8092);
        LOGGER.info("odb-manager deployed on port " + 8080);
    }

    private void getData(String input, Handler<AsyncResult<String>> readyHandler) {
        IDSMessage idsMessage = IDSMessageParser.parse(input).orElse(new IDSMessage(null, null));

        if (!idsMessage.getHeader().isPresent()) {
            try {
                idsService.handleRejectionMessage(RejectionReason.MALFORMED_MESSAGE, new URI(String.valueOf(RejectionReason.MALFORMED_MESSAGE)), readyHandler);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Message header = idsMessage.getHeader().get();
            URI uri = idsMessage.getHeader().get().getId();
                try {
                    if (header instanceof DescriptionRequestMessage) {
                        LOGGER.info("DescriptionRequestMessage received.");
                        idsService.getSelfDescriptionResponse(uri, (DescriptionRequestMessage)header, readyHandler);
                    } else if(idsMessage.getPayload().isPresent()) {
                        String payload = idsMessage.getPayload().get();
                        if (header instanceof ConnectorAvailableMessage) {
                            LOGGER.info("AvailableMessage received.");
                            Connector connector = Json.decodeValue(payload, Connector.class);
                            registerController.register(uri, connector, readyHandler);
                        } else if (header instanceof ConnectorUnavailableMessage) {
                            LOGGER.info("UnavailableMessage received.");
                            Connector connector = Json.decodeValue(payload, Connector.class);
                            unregisterController.unregister(uri, connector, readyHandler);
                        } else if (header instanceof ConnectorUpdateMessage) {
                            LOGGER.info("UpdateMessage received.");
                            Connector connector = Json.decodeValue(payload, Connector.class);
                            updateController.update(uri, connector, readyHandler);
                        } else if (header instanceof QueryMessage) {
                            LOGGER.info("QueryMessage received.");
                            queryMessageController.queryMessage(payload, uri, readyHandler);
                        } else {
                            LOGGER.error(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED);
                            idsService.handleRejectionMessage(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED, uri, readyHandler);
                        }
                    } else {
                        LOGGER.error(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED);
                        idsService.handleRejectionMessage(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED, uri, readyHandler);
                    }
                } catch (Exception e) {
                    LOGGER.error("Something went wrong while parsing the IDS message.",e);
                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                }
        }

    }

    private void about(Handler<AsyncResult<String>> resultHandler) {
        JsonObject jsonObject = new JsonObject();
        idsService.buildBroker(jsonObject, brokerResult -> {
            if(brokerResult.succeeded()) {
                resultHandler.handle(Future.succeededFuture(Json.encode(brokerResult.result())));
            } else {
                resultHandler.handle(Future.failedFuture(brokerResult.cause()));
            }
        });
    }

    private void reply(AsyncResult result, HttpServerResponse response) {
        if (result.succeeded() && result.result() != null) {
            String entity = result.result().toString();
            if (!response.headWritten()) {
                response.putHeader("content-type", ContentType.APPLICATION_JSON.toString());
                response.end(entity);
            }
        } else {
            response.setStatusCode(404).end();
        }
    }

    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }
}
