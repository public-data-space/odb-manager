package de.fraunhofer.fokus.ids.main;

import de.fraunhofer.fokus.ids.controller.*;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.authService.AuthAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageServiceVerticle;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseServiceVerticle;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerServiceVerticle;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.fokus.ids.utils.InitService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.fokus.ids.utils.models.IDSMessage;
import de.fraunhofer.fokus.ids.utils.services.authService.AuthAdapterService;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import org.apache.http.entity.ContentType;

import java.io.IOException;
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
    private Serializer serializer;
    private int servicePort;
    private AuthAdapterService authAdapterService;

    @Override
    public void start(Promise<Void> startPromise) {
        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");
        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);
        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(config -> {
            if (config.succeeded()){

                DeploymentOptions deploymentOptions = new DeploymentOptions();
                deploymentOptions.setWorker(true);
                Future<String> deployment = Future.succeededFuture();
                deployment.compose(id1 -> {
                    Promise<String> dcatTransformer = Promise.promise();
                    Future<String> dcatTransformerFuture = dcatTransformer.future();
                    vertx.deployVerticle(DCATTransformerServiceVerticle.class.getName(), deploymentOptions, dcatTransformerFuture);
                    return dcatTransformerFuture;
                }).compose(id2 -> {
                    Promise<String> brokerMessage = Promise.promise();
                    Future<String> brokerMessageFuture = brokerMessage.future();
                    vertx.deployVerticle(PiveauMessageServiceVerticle.class.getName(), deploymentOptions, brokerMessageFuture);
                    return brokerMessageFuture;
                }).compose(id3 -> {
                    Promise<String> databaseMessage = Promise.promise();
                    Future<String> databaseMessageFuture = databaseMessage.future();
                    vertx.deployVerticle(DatabaseServiceVerticle.class.getName(), deploymentOptions, databaseMessage);
                    return databaseMessageFuture;
                }).compose(id4 -> {
                    Promise<String> authPromise= Promise.promise();
                    Future<String> authFuture = authPromise.future();
                    vertx.deployVerticle(AuthAdapterServiceVerticle.class.getName(), deploymentOptions, authFuture);
                    return authFuture;
                }).setHandler(ar -> {
                    if (ar.succeeded()) {
                        Future initFuture = Promise.promise().future();
                        new InitService(vertx).initDatabase(initFuture);
                        if (initFuture.succeeded()) {

                            this.serializer = new Serializer();
                            WebClient webClient = WebClient.create(vertx);
                            CircuitBreaker breaker = CircuitBreaker.create("virtuoso-breaker", vertx, new CircuitBreakerOptions().setMaxRetries(5))
                                    .retryPolicy(count -> count * 1000L);
                            this.tsConnector = TSConnector.create(webClient, breaker, config.result().getJsonObject("VIRTUOSO_CONFIG"));
                            this.queryMessageController = new QueryMessageController(tsConnector, vertx);
                            GraphManager graphManager = new GraphManager(vertx, tsConnector);
                            this.updateController = new UpdateController(vertx, graphManager,tsConnector);
                            this.unregisterController = new UnregisterController(vertx, graphManager,tsConnector);
                            this.registerController = new RegisterController(vertx,graphManager,tsConnector);
                            this.servicePort = config.result().getInteger("SERVICE_PORT");
                            this.idsService = new IDSService(vertx,tsConnector);
                            this.authAdapterService = AuthAdapterService.createProxy(vertx, AuthAdapterServiceVerticle.ADDRESS);

                            router = Router.router(vertx);
                            createHttpServer(vertx);
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
        router.post("/infrastructure").handler(routingContext -> getInfrastructure(routingContext.getBodyAsString(),
                reply -> reply(reply, routingContext.response())));
        router.post("/data").handler(routingContext -> getData(routingContext.getBodyAsString(),
                reply -> reply(reply, routingContext.response())));
        router.route("/about").handler(routingContext -> about(reply -> reply(reply, routingContext.response())));
        router.route("/").handler(routingContext -> about(reply -> reply(reply, routingContext.response())));
        LOGGER.info("Starting odb-manager ");
        server.requestHandler(router).listen(this.servicePort);
        LOGGER.info("odb-manager deployed on port " + this.servicePort);
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
            authAdapterService.isAuthenticated(header.getSecurityToken().getTokenValue(), authreply -> {
                if (authreply.succeeded()) {
                    try {
                        if (idsMessage.getPayload().isPresent()) {
                            String payload = idsMessage.getPayload().get();
                            if (header instanceof QueryMessage) {
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
                        LOGGER.error("Something went wrong while parsing the IDS message.", e);
                        idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                    }
                } else {
                    idsService.handleRejectionMessage(RejectionReason.NOT_AUTHENTICATED, uri, readyHandler);
                }
            });
        }
    }

    private void getInfrastructure(String input, Handler<AsyncResult<String>> readyHandler) {
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
            authAdapterService.isAuthenticated(header.getSecurityToken().getTokenValue(), authreply -> {
                if (authreply.succeeded()) {
                    try {
                        if (header instanceof DescriptionRequestMessage) {
                            LOGGER.info("DescriptionRequestMessage received.");
                            idsService.getSelfDescriptionResponse(uri, (DescriptionRequestMessage)header, readyHandler);
                        } else if(idsMessage.getPayload().isPresent()) {
                            String payload = idsMessage.getPayload().get();
                            if (header instanceof ConnectorAvailableMessage) {
                                LOGGER.info("AvailableMessage received.");
                                Connector connector = serializer.deserialize(payload, Connector.class);
                                registerController.register(uri, connector, readyHandler);
                            } else if (header instanceof ConnectorUnavailableMessage) {
                                LOGGER.info("UnavailableMessage received.");
                                Connector connector = serializer.deserialize(payload, Connector.class);
                                unregisterController.unregister(uri, connector, readyHandler);
                            } else if (header instanceof ConnectorUpdateMessage) {
                                LOGGER.info("UpdateMessage received.");
                                Connector connector = serializer.deserialize(payload, Connector.class);
                                updateController.update(uri, connector, readyHandler);
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
                } else {
                    idsService.handleRejectionMessage(RejectionReason.NOT_AUTHENTICATED, uri, readyHandler);
                }
            });
        }

    }


    private void about(Handler<AsyncResult<String>> resultHandler) {
        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(config -> {
            if(config.succeeded()){
                idsService.buildBroker(config.result().getJsonObject("BROKER_CONFIG"), brokerResult -> {
                    if (brokerResult.succeeded()) {
                        try {
                            resultHandler.handle(Future.succeededFuture(serializer.serialize(brokerResult.result())));
                        } catch (IOException e) {
                            LOGGER.error(e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        resultHandler.handle(Future.failedFuture(brokerResult.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.failedFuture(config.cause()));
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
