package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.UUID;

public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
    }

    public void getData (String input, Handler<AsyncResult<Void>> readyHandler){
        ConnectorNotificationMessage header = IDSMessageParser.getHeader(input);
        Connector connector = IDSMessageParser.getBody(input);
        try {
            if (header instanceof ConnectorAvailableMessage) {
                register(connector, readyHandler);
            } else if (header instanceof ConnectorUnavailableMessage) {
                unregister(connector, readyHandler);
            } else if (header instanceof ConnectorUpdateMessage) {
                update(connector, readyHandler);
            } else {
                LOGGER.error("Invalid message signature.");
            }
        }
        catch (Exception e){
            LOGGER.error("Something went wrong while parsing the IDS message.");
        }
    }

    private void update(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        String datasetId = ""; //Get ID of dataset from somwhere
        String catalogueId = ""; //Get ID of catalogue from somwhere
        brokerMessageService.createCatalogue(toJson(connector),catalogueId, catalogueReply -> {
            if(catalogueReply.succeeded()){
                brokerMessageService.createDataSet(toJson(connector), datasetId, catalogueId, datasetReply -> handleReply(datasetReply, readyHandler));
            } else {
                LOGGER.error(catalogueReply.cause());
                readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
            }
        });
    }

    private void register(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        String catalogueid = UUID.randomUUID().toString();
        brokerMessageService.createCatalogue(toJson(connector),catalogueid, catalogueReply -> {
            if(catalogueReply.succeeded()){
                String datasetid = UUID.randomUUID().toString();
                brokerMessageService.createDataSet(toJson(connector), datasetid, catalogueid, datasetReply -> handleReply(datasetReply, readyHandler));
            } else {
                LOGGER.error(catalogueReply.cause());
                readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
            }
        });
    }

    private void unregister(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        String datasetId = ""; //Get ID of dataset from somwhere
        String catalogueId = ""; //Get ID of catalogue from somwhere
        brokerMessageService.deleteDataSet(datasetId, catalogueId, datasetDeleteReply -> {
            if(datasetDeleteReply.succeeded()){
                brokerMessageService.deleteCatalogue(catalogueId, datasetReply -> handleReply(datasetReply, readyHandler));
            } else {
                LOGGER.error(datasetDeleteReply.cause());
            }
        });
    }

    private void handleReply(AsyncResult<BrokerMessageService> datasetReply, Handler<AsyncResult<Void>> readyHandler) {
        if(datasetReply.succeeded()){
            LOGGER.info("success");
            readyHandler.handle(Future.succeededFuture());
        } else {
            LOGGER.error(datasetReply.cause());
            readyHandler.handle(Future.failedFuture(datasetReply.cause()));
        }
    }

    private JsonObject toJson(Object object){
        try {
            return new JsonObject(Json.encode(object));
        } catch (Exception e){
            LOGGER.error("Something went wrong while parsing the IDS Connector.");
        }
        return null;
    }
}
