package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;
    private DCATTransformerService dcatTransformerService;

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
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
            e.printStackTrace();
            LOGGER.error("Something went wrong while parsing the IDS message.");
        }
    }

    private void update(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        String datasetId = ""; //Get ID of dataset from somwhere
        String catalogueId = ""; //Get ID of catalogue from somwhere

        Future<String> catalogueFuture = Future.future();
        List<Future> datassetFutures = new ArrayList<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        catalogueFuture.setHandler( reply -> {
            if(reply.succeeded()) {
                brokerMessageService.createCatalogue(reply.result(), catalogueId, catalogueReply -> {
                    if (catalogueReply.succeeded()) {
                        CompositeFuture.all(datassetFutures).setHandler( dataassetCreateReply -> {
                           if(dataassetCreateReply.succeeded()){
                                for(Future<String> dataassetFuture : datassetFutures){
                                    brokerMessageService.createDataSet(dataassetFuture.result(), datasetId, catalogueId, datasetReply -> {});
                                }
                               readyHandler.handle(Future.succeededFuture());
                           } else {
                            LOGGER.error(dataassetCreateReply.cause());
                            readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
                           }
                        });
                    } else {
                        LOGGER.error(catalogueReply.cause());
                        readyHandler.handle(Future.failedFuture(catalogueReply.cause()));

                    }
                });
            } else {
                LOGGER.error(reply.cause());
                readyHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void register(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        Future<String> catalogueFuture = Future.future();
        List<Future> datassetFutures = new ArrayList<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        catalogueFuture.setHandler( reply -> {
            if(reply.succeeded()) {
                String catalogueId = UUID.randomUUID().toString();
                brokerMessageService.createCatalogue(reply.result(), catalogueId, catalogueReply -> {
                    if (catalogueReply.succeeded()) {
                        CompositeFuture.all(datassetFutures).setHandler( dataassetCreateReply -> {
                            if(dataassetCreateReply.succeeded()){
                                for(Future<String> dataassetFuture : datassetFutures){
                                    String datasetId = UUID.randomUUID().toString();
                                    brokerMessageService.createDataSet(dataassetFuture.result(), datasetId, catalogueId, datasetReply -> {});
                                }
                                readyHandler.handle(Future.succeededFuture());
                            } else {
                                LOGGER.error(dataassetCreateReply.cause());
                                readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
                            }
                        });
                    } else {
                        LOGGER.error(catalogueReply.cause());
                        readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
                    }
                });
            } else {
                LOGGER.error(reply.cause());
                readyHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void unregister(Connector connector, Handler<AsyncResult<Void>> readyHandler) {
        String datasetId = ""; //Get ID of dataset from somwhere
        String catalogueId = ""; //Get ID of catalogue from somwhere
        brokerMessageService.deleteDataSet(datasetId, catalogueId, datasetDeleteReply -> {
            if(datasetDeleteReply.succeeded()){
                brokerMessageService.deleteCatalogue(catalogueId, datasetReply -> {});
                readyHandler.handle(Future.succeededFuture());
            } else {
                LOGGER.error(datasetDeleteReply.cause());
            }
        });
    }

    private void initTransformations(Connector connector, Future<String> catalogueFuture, List<Future> datassetFutures){
        String con = Json.encode(connector);
        dcatTransformerService.transformCatalogue(con, catalogueFuture.completer());
        if(connector.getCatalog() != null) {
            for (Resource resource : connector.getCatalog().getOffer()) {
                Future<String> dataassetFuture = Future.future();
                datassetFutures.add(dataassetFuture);
                dcatTransformerService.transformDataset(Json.encode(resource), dataassetFuture.completer());
            }
        }
    }

}
