package de.fraunhofer.fokus.ids.services;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
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
    private final Logger LOGGER = LoggerFactory.getLogger(IDSService.class.getName());
    private CatalogueManager catalogueManager;
    private String INFO_MODEL_VERSION = "2.0.0";
    private String[] SUPPORTED_INFO_MODEL_VERSIONS = {"2.0.0"};
    private TSConnector tsConnector ;
    public IDSService(Vertx vertx , TSConnector tsConnector) {
        this.catalogueManager = new CatalogueManager(vertx);
        this.tsConnector = tsConnector;
    }

    private void createSucceededMessage(URI correlationMessageURI, Handler<AsyncResult<MessageProcessedNotificationMessage>> resultHandler) {
        try {
            String uuid = UUID.randomUUID().toString();
            MessageProcessedNotificationMessage message = new MessageProcessedNotificationMessageBuilder(new URI(uuid))
                    ._correlationMessage_(correlationMessageURI)
                    ._issued_(getDate())
                    ._modelVersion_("2.0.0")
                    ._issuerConnector_(new URI("URI"))
                    ._securityToken_(new DynamicAttributeTokenBuilder()
                            ._tokenFormat_(TokenFormat.JWT)
                            ._tokenValue_(getJWT())
                            .build())
                    .build();
            resultHandler.handle(Future.succeededFuture(message));
        } catch (URISyntaxException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    public ResultMessage createResultMessage(URI correlationMessageURI){
        try {
            return new ResultMessageBuilder()
                    ._correlationMessage_(correlationMessageURI)
                    ._modelVersion_(INFO_MODEL_VERSION)
                    ._issued_(getDate())
                    ._issuerConnector_(new URI("URI"))
                    ._securityToken_(new DynamicAttributeTokenBuilder()
                            ._tokenFormat_(TokenFormat.JWT)
                            ._tokenValue_(getJWT())
                            .build())
                    .build();
        } catch (URISyntaxException e) {
            LOGGER.error(e);
        }
        return null;
    }

    private void createRejectionMessage(RejectionReason rejectionReason, URI correlationMessageURI, Handler<AsyncResult<RejectionMessage>> resultHandler) {
        try {
            String uuid = UUID.randomUUID().toString();
            RejectionMessage message = new RejectionMessageBuilder(new URI(uuid))
                    ._correlationMessage_(correlationMessageURI)
                    ._issued_(getDate())
                    ._modelVersion_(INFO_MODEL_VERSION)
                    ._issuerConnector_(new URI("URI"))
                    ._securityToken_(new DynamicAttributeTokenBuilder()
                            ._tokenFormat_(TokenFormat.JWT)
                            ._tokenValue_(getJWT())
                            .build())
                    ._rejectionReason_(rejectionReason)
                    .build();
            resultHandler.handle(Future.succeededFuture(message));
        } catch (URISyntaxException e) {
            LOGGER.error(e);
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private DescriptionResponseMessage createSelfDescriptionResponse(JsonObject jsonObject, URI correlationMessageURI) {

        try {
            return new DescriptionResponseMessageBuilder(new URI("broker#SelfDescriptionResponse"))
                    ._issued_(getDate())
                    ._correlationMessage_(correlationMessageURI)
                    ._modelVersion_(INFO_MODEL_VERSION)
                    .build();
        } catch (URISyntaxException e) {
            LOGGER.error(e);
        }
        return null;
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

    private String getJWT() {
        return "abcdefg12";
    }

    public void getSelfDescriptionResponse(URI uri,JsonObject header, Handler<AsyncResult<String>> resultHandler) {
        DescriptionResponseMessage selfDescriptionResponse = createSelfDescriptionResponse(header, uri);
        if (header.getString("requestedElement")!=null) {
                tsConnector.getGraph(header.getString("requestedElement"),asyncResult->{
                    if (asyncResult.succeeded()){
                        createMultiPartMessage(uri, selfDescriptionResponse, new JsonObject(asyncResult.result()), resultHandler);
                    }
                    else {
                        LOGGER.error(asyncResult.cause());
                        handleRejectionMessage(RejectionReason.NOT_FOUND,uri,resultHandler);
                    }
                });
        }
        else {
            buildBroker(header, brokerResult -> {
                if(brokerResult.succeeded()) {
                    createMultiPartMessage(uri, selfDescriptionResponse, brokerResult.result(), resultHandler);
                } else {
                    LOGGER.error(brokerResult.cause());
                    resultHandler.handle(Future.failedFuture(brokerResult.cause()));
                }
            });
        }
    }

    public void buildBroker(JsonObject config, Handler<AsyncResult<Broker>> next) {
        listOfExternalIds(arrayListAsyncResult -> {
            if(arrayListAsyncResult.succeeded()) {
                Optional<Broker> brokerOptional = createBroker(config, arrayListAsyncResult.result());
                if(brokerOptional.isPresent()) {
                    next.handle(Future.succeededFuture(brokerOptional.get()));
                }
                else {
                    LOGGER.error("Optional not present.");
                    next.handle(Future.failedFuture("Optional not present."));
                }
            } else {
                next.handle(Future.failedFuture(arrayListAsyncResult.cause()));
            }
        });
    }

    private Optional<Broker> createBroker(JsonObject config, ArrayList<URI> connectorURIs){
        try {
            return  Optional.of(new BrokerBuilder((new URI("payload#Broker")))
                    ._maintainer_(new URI("maintainer"))
                    ._version_("0.0.1")
                    ._curator_(new URI("curator"))
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

        ContentBody contentBody = new StringBody(Json.encodePrettily(headerObject), ContentType.create("application/json"));
        ContentBody payload = new StringBody(Json.encodePrettily(payloadObject), ContentType.create("application/json"));

        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                .setBoundary("IDSMSGPART")
                .setCharset(StandardCharsets.UTF_8)
                .setContentType(ContentType.APPLICATION_JSON)
                .addPart("header", contentBody)
                .addPart("payload", payload);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            multipartEntityBuilder.build().writeTo(out);
            resultHandler.handle(Future.succeededFuture(out.toString()));
        } catch (IOException e) {
            LOGGER.error(e);
            handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR,uri,resultHandler);
        }
    }

    private Buffer createMultipartMessage(Message message, Connector connector) {
        ContentBody cb = new StringBody(Json.encodePrettily(message), org.apache.http.entity.ContentType.create("application/json"));
        ContentBody result = new StringBody(Json.encodePrettily(connector), org.apache.http.entity.ContentType.create("application/json"));

        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create()
                .setBoundary("IDSMSGPART")
                .setCharset(StandardCharsets.UTF_8)
                .setContentType(ContentType.APPLICATION_JSON)
                .addPart("header", cb);
        //.addPart("payload", result);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
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
                Buffer buffer = createMultipartMessage(messageProcessedNotificationAsyncResult.result(), null);
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(messageProcessedNotificationAsyncResult.cause()));
            }
        });
    }

    public void handleRejectionMessage(RejectionReason rejectionReason, URI uri, Handler<AsyncResult<String>> readyHandler) {
        createRejectionMessage(rejectionReason, uri, rejectionMessageAsyncResult -> {
            if (rejectionMessageAsyncResult.succeeded()) {
                Buffer buffer = createMultipartMessage(rejectionMessageAsyncResult.result(), null);
                readyHandler.handle(Future.succeededFuture(buffer.toString()));
            } else {
                readyHandler.handle(Future.failedFuture(rejectionMessageAsyncResult.cause()));
            }
        });
    }

}