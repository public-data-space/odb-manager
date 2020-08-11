package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.services.authService.AuthAdapterServiceVerticle;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.fokus.ids.utils.services.authService.AuthAdapterService;
import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.StringBody;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class IDSService {
    private static final String VERSION_NUMBER = "1.0.0";
    private final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());
    private CatalogueManager catalogueManager;
    private String INFO_MODEL_VERSION = "3.1.0";
    private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"3.1.0"};
    private TSConnector tsConnector ;
    private AuthAdapterService authAdapterService;
    private Vertx vertx;
    private Serializer serializer= new Serializer();

    public IDSService(Vertx vertx , TSConnector tsConnector) {
        this.catalogueManager = new CatalogueManager(vertx);
        this.tsConnector = tsConnector;
        this.vertx = vertx;
        this.authAdapterService = AuthAdapterService.createProxy(vertx, AuthAdapterServiceVerticle.ADDRESS);
    }

    private void createSucceededMessage(URI correlationMessageURI, Handler<AsyncResult<MessageProcessedNotificationMessage>> resultHandler) {
        authAdapterService.retrieveToken( tokenReply -> {
            if(tokenReply.succeeded()){
                getConfiguration( reply -> {
                    if(reply.succeeded()) {
                        try {
                            MessageProcessedNotificationMessage message = new MessageProcessedNotificationMessageBuilder(new URI(reply.result().getString("baseUrl")+"/MessageProcessedNotification/"+UUID.randomUUID()))
                                    ._correlationMessage_(correlationMessageURI)
                                    ._issued_(getDate())
                                    ._modelVersion_(INFO_MODEL_VERSION)
                                    ._issuerConnector_(new URI(reply.result().getString("baseUrl")+"#Broker"))
                                    ._securityToken_(new DynamicAttributeTokenBuilder(new URI(reply.result().getString("baseUrl")+"#DAT"))
                                            ._tokenFormat_(TokenFormat.JWT)
                                            ._tokenValue_(tokenReply.result())
                                            .build())
                                    .build();
                            resultHandler.handle(Future.succeededFuture(message));
                        } catch (URISyntaxException e) {
                            LOGGER.error(e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.failedFuture(tokenReply.cause()));
            }
        });
    }

    public void createResultMessage(URI correlationMessageURI, Handler<AsyncResult<ResultMessage>> resultHandler){
        authAdapterService.retrieveToken( tokenReply -> {
            if(tokenReply.succeeded()){
                getConfiguration( reply -> {
                    if(reply.succeeded()) {
                        try {
                            ResultMessage message =  new ResultMessageBuilder(new URI(reply.result().getString("baseUrl")+"/ResultMessage/"+UUID.randomUUID()))
                                    ._correlationMessage_(correlationMessageURI)
                                    ._modelVersion_(INFO_MODEL_VERSION)
                                    ._issued_(getDate())
                                    ._issuerConnector_(new URI(reply.result().getString("baseUrl")+"#Broker"))
                                    ._securityToken_(new DynamicAttributeTokenBuilder(new URI(reply.result().getString("baseUrl")+"#DAT"))
                                            ._tokenFormat_(TokenFormat.JWT)
                                            ._tokenValue_(tokenReply.result())
                                            .build())
                                    .build();
                            resultHandler.handle(Future.succeededFuture(message));
                        } catch (URISyntaxException e) {
                            LOGGER.error(e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.failedFuture(tokenReply.cause()));
            }
        });
    }

    private void createRejectionMessage(RejectionReason rejectionReason, URI correlationMessageURI, Handler<AsyncResult<RejectionMessage>> resultHandler) {
        authAdapterService.retrieveToken( tokenReply -> {
            if(tokenReply.succeeded()){
                getConfiguration( reply -> {
                    if(reply.succeeded()) {
                        try {
                            RejectionMessage message = new RejectionMessageBuilder(new URI(reply.result().getString("baseUrl")+"/RejectionMessage/"+UUID.randomUUID()))
                                    ._correlationMessage_(correlationMessageURI)
                                    ._issued_(getDate())
                                    ._modelVersion_(INFO_MODEL_VERSION)
                                    ._issuerConnector_(new URI(reply.result().getString("baseUrl")+"#Broker"))
                                    ._securityToken_(new DynamicAttributeTokenBuilder(new URI(reply.result().getString("baseUrl")+"#DAT"))
                                            ._tokenFormat_(TokenFormat.JWT)
                                            ._tokenValue_(tokenReply.result())
                                            .build())
                                    ._rejectionReason_(rejectionReason)
                                    .build();
                            resultHandler.handle(Future.succeededFuture(message));
                        } catch (URISyntaxException e) {
                            LOGGER.error(e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        LOGGER.error(reply.cause());
                        resultHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                resultHandler.handle(Future.failedFuture(tokenReply.cause()));
            }
        });
    }

    private void createSelfDescriptionResponse( JsonObject config, URI correlationMessageURI, Handler<AsyncResult<Message>> resultHandler) {
        authAdapterService.retrieveToken( tokenReply -> {
            if(tokenReply.succeeded()){
                try {
                    resultHandler.handle(Future.succeededFuture(new DescriptionResponseMessageBuilder(new URI(config.getString("baseUrl")+"/DescriptionResponseMessage/"+UUID.randomUUID()))
                            ._issued_(getDate())
                            ._issuerConnector_(new URI(config.getString("baseUrl")+"#Broker"))
                            ._correlationMessage_(correlationMessageURI)
                            ._modelVersion_(INFO_MODEL_VERSION)
                            ._securityToken_(new DynamicAttributeTokenBuilder(new URI(config.getString("baseUrl")+"#DAT"))
                                    ._tokenFormat_(TokenFormat.JWT)
                                    ._tokenValue_(tokenReply.result())
                                    .build())
                            .build()));
                } catch (URISyntaxException e) {
                    LOGGER.error(e);
                }
            } else {
                resultHandler.handle(Future.failedFuture(tokenReply.cause()));
            }
        });
    }

    private XMLGregorianCalendar getDate() {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(new Date());
        try {
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
        } catch (DatatypeConfigurationException e) {
            LOGGER.error(e);
        }
        return null;
    }

    public void getSelfDescriptionResponse(URI uri,DescriptionRequestMessage header, Handler<AsyncResult<String>> resultHandler) {
        getConfiguration( reply -> {
            if (reply.succeeded()) {
                createSelfDescriptionResponse(reply.result(), uri, selfDescriptionReply -> {
                    if (selfDescriptionReply.succeeded()) {
                        if (header.getRequestedElement() != null) {
                            tsConnector.getGraph(header.getRequestedElement().toString(), asyncResult -> {
                                if (asyncResult.succeeded()) {
                                    createMultiPartMessage(uri, selfDescriptionReply.result(), new JsonObject(asyncResult.result()), resultHandler);
                                } else {
                                    LOGGER.error(asyncResult.cause());
                                    handleRejectionMessage(RejectionReason.NOT_FOUND, uri, resultHandler);
                                }
                            });
                        } else {
                            buildBroker(reply.result(), brokerResult -> {
                                if (brokerResult.succeeded()) {
                                    createMultiPartMessage(uri, selfDescriptionReply.result(), brokerResult.result(), resultHandler);
                                } else {
                                    LOGGER.error(brokerResult.cause());
                                    resultHandler.handle(Future.failedFuture(brokerResult.cause()));
                                }
                            });
                        }
                    } else {
                        LOGGER.error("SDR Optional not present.");
                        resultHandler.handle(Future.failedFuture("SDR Optional not present."));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                resultHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    public void buildBroker(JsonObject config, Handler<AsyncResult<Broker>> next) {
        listOfExternalIds(arrayListAsyncResult -> {
            if(arrayListAsyncResult.succeeded()) {
                Optional<Broker> brokerOptional = createBroker(config, arrayListAsyncResult.result());
                if(brokerOptional.isPresent()) {
                    next.handle(Future.succeededFuture(brokerOptional.get()));
                }
                else {
                    LOGGER.error("Broker Optional not present.");
                    next.handle(Future.failedFuture("Broker Optional not present."));
                }
            } else {
                next.handle(Future.failedFuture(arrayListAsyncResult.cause()));
            }
        });
    }

    private Optional<Broker> createBroker(JsonObject config, ArrayList<URI> connectorURIs){
        try {
            return Optional.of(new BrokerBuilder(new URI(config.getString("baseUrl")+"#Broker"))
                    ._maintainer_(new URI(config.getString("maintainer")))
                    ._version_(VERSION_NUMBER)
                    ._curator_(new URI(config.getString("curator")))
                    ._connector_(connectorURIs)
                    ._outboundModelVersion_(INFO_MODEL_VERSION)
                    ._inboundModelVersion_(new ArrayList<>(Arrays.asList(SUPPORTED_INFO_MODEL_VERSIONS)))
                    .build());
        } catch (URISyntaxException e) {
            LOGGER.error(e);
        }
        return Optional.empty();
    }

    private void listOfExternalIds(Handler<AsyncResult<ArrayList<URI>>> next) {
        ArrayList<URI> externalIds = new ArrayList<>();
        catalogueManager.find(data -> {
            if (data.succeeded()) {
                for (JsonObject jsonObject : data.result()) {
                    try {
                        externalIds.add(new URI(jsonObject.getString("external_id")));
                    } catch (URISyntaxException e) {
                        e.printStackTrace();
                    }
                }
                next.handle(Future.succeededFuture(externalIds));
            } else {
                LOGGER.error(data.cause());
                next.handle(Future.failedFuture(data.cause()));
            }
        });
    }

    public void createMultiPartMessage(URI uri, Object headerObject, Object payloadObject, Handler<AsyncResult<String>> resultHandler) {
        try {
            ContentBody contentBody = new StringBody(serializer.serialize(headerObject), ContentType.create("application/json"));
            ContentBody payload = new StringBody(serializer.serialize(payloadObject), ContentType.create("application/json"));

            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                    .setBoundary("msgpart")
                    .setCharset(StandardCharsets.UTF_8)
                    .setContentType(ContentType.APPLICATION_JSON)
                    .addPart("header", contentBody)
                    .addPart("payload", payload);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            multipartEntityBuilder.build().writeTo(out);
            resultHandler.handle(Future.succeededFuture(out.toString()));
        } catch (IOException e) {
            LOGGER.error(e);
            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR,uri,resultHandler);
        }
    }

    private Buffer createMultipartMessage(Message message) {
        try {
            ContentBody cb = new StringBody(serializer.serialize(message), org.apache.http.entity.ContentType.create("application/json"));

            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                    .setBoundary("msgpart")
                    .setCharset(StandardCharsets.UTF_8)
                    .setContentType(ContentType.APPLICATION_JSON)
                    .addPart("header", cb);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            multipartEntityBuilder.build().writeTo(out);
            return Buffer.buffer().appendString(out.toString());
        } catch (IOException e) {
            LOGGER.error(e);
        }
        return null;
    }

    public void handleSucceededMessage(URI uri, Handler<AsyncResult<String>> readyHandler) {
        createSucceededMessage(uri, messageProcessedNotificationAsyncResult -> {
            if (messageProcessedNotificationAsyncResult.succeeded()) {
                Buffer buffer = createMultipartMessage(messageProcessedNotificationAsyncResult.result());
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(messageProcessedNotificationAsyncResult.cause()));
            }
        });
    }

    public void handleRejectionMessage(RejectionReason rejectionReason, URI uri, Handler<AsyncResult<String>> readyHandler) {
        createRejectionMessage(rejectionReason, uri, rejectionMessageAsyncResult -> {
            if (rejectionMessageAsyncResult.succeeded()) {
                Buffer buffer = createMultipartMessage(rejectionMessageAsyncResult.result());
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(rejectionMessageAsyncResult.cause()));
            }
        });
    }


    private void getConfiguration(Handler<AsyncResult<JsonObject>> resultHandler){

        ConfigStoreOptions confStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(confStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(config -> {
            if(config.succeeded()){
                resultHandler.handle(Future.succeededFuture(config.result().getJsonObject("BROKER_CONFIG")));
            } else {
                resultHandler.handle(Future.failedFuture(config.cause()));
            }
        });
    }

}