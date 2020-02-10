package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.Map;

public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;
    private DCATTransformerService dcatTransformerService;
    private DatabaseService databaseService;

    private final static String INSERT_STATEMENT = "INSERT INTO ? values (NOE(),NOW(),?,?)";
    private final static String SELECT_STATEMENT = "SELECT * FROM ? WHERE external_id=?";

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
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
        Future<List<JsonObject>> catalogueIdFuture = Future.future();
        databaseService.query(SELECT_STATEMENT, new JsonArray().add("catalogues").add(connector.getId()), cataloguePersistenceReply -> catalogueIdFuture.completer());
        Future<String> catalogueFuture = Future.future();
        java.util.Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        CompositeFuture.all(catalogueFuture,catalogueIdFuture).setHandler( reply -> {
            if(reply.succeeded()) {
                brokerMessageService.createCatalogue(catalogueFuture.result(), catalogueIdFuture.result().get(0).getString("internal_id"), catalogueReply -> {
                    if (catalogueReply.succeeded()) {
                        CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler(dataassetCreateReply -> {
                           if(dataassetCreateReply.succeeded()){
                                for(String dataassetId: datassetFutures.keySet()) {
                                    databaseService.query(SELECT_STATEMENT, new JsonArray().add("datasets").add(dataassetId), datasetPersistenceReply -> {
                                        if (datasetPersistenceReply.succeeded()) {
                                            brokerMessageService.createDataSet(datassetFutures.get(dataassetId).result(), datasetPersistenceReply.result().get(0).getString("internal_id"), catalogueIdFuture.result().get(0).getString("external_id"), datasetReply -> {
                                            });
                                        } else {

                                        }
                                    });
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
        Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        catalogueFuture.setHandler( reply -> {
            if(reply.succeeded()) {
                String catalogueId = UUID.randomUUID().toString();
                brokerMessageService.createCatalogue(reply.result(), catalogueId, catalogueReply -> {
                    if (catalogueReply.succeeded()) {
                        databaseService.update(INSERT_STATEMENT, new JsonArray().add("catalogues").add(connector.getId()).add(catalogueId), cataloguePersistenceReply -> {});
                        CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler( dataassetCreateReply -> {
                            if(dataassetCreateReply.succeeded()){
                                for(String datasetExternalId : datassetFutures.keySet()){
                                    String datasetId = UUID.randomUUID().toString();
                                    brokerMessageService.createDataSet(datassetFutures.get(datasetExternalId).result(), datasetExternalId, catalogueId, datasetReply -> {});
                                    databaseService.update(INSERT_STATEMENT, new JsonArray().add("datasets").add(Json.decodeValue(datassetFutures.get(datasetExternalId).result(), Resource.class).getId()).add(datasetId), datasetPersistenceReply -> {});
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
        Future<String> catalogueIdFuture = Future.future();
        databaseService.query(SELECT_STATEMENT, new JsonArray().add("catalogues").add(connector.getId()), cataloguePersistenceReply -> catalogueIdFuture.completer());

        catalogueIdFuture.setHandler(catalogueIdReply -> {
            if(catalogueIdReply.succeeded()) {
                List<Future> datasetDeleteFutures = new ArrayList<>();

                for (Resource dataasset : connector.getCatalog().getOffer()) {
                    databaseService.query(SELECT_STATEMENT, new JsonArray().add("dataset").add(dataasset.getId()), datasetIdreply -> {
                        if (datasetIdreply.succeeded()) {
                            Future datasetDeleteFuture = Future.future();
                            datasetDeleteFutures.add(datasetDeleteFuture);
                            brokerMessageService.deleteDataSet(datasetIdreply.result().get(0).getString("internal_id"), catalogueIdReply.result(), datasetDeleteFuture.completer());
                        } else {

                        }
                    });
                }
                CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
                    if (reply.succeeded()) {
                        brokerMessageService.deleteCatalogue(catalogueIdReply.result(), datasetReply -> {
                        });
                        readyHandler.handle(Future.succeededFuture());
                    } else {
                        LOGGER.error(reply.cause());
                    }
                });
            } else {

            }
    });
    }

    private void initTransformations(Connector connector, Future<String> catalogueFuture, Map<String, Future<String>> datassetFutures){
        String con = Json.encode(connector);
        dcatTransformerService.transformCatalogue(con, catalogueFuture.completer());
        if(connector.getCatalog() != null) {
            for (Resource resource : connector.getCatalog().getOffer()) {
                Future<String> dataassetFuture = Future.future();
                datassetFutures.put(resource.getId().toString(), dataassetFuture);
                dcatTransformerService.transformDataset(Json.encode(resource), dataassetFuture.completer());
            }
        }
    }
}
