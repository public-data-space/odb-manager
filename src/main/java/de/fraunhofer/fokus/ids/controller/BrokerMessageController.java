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

    private final static String INSERT_CAT_STATEMENT = "INSERT INTO catalogues (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";
    private final static String INSERT_DS_STATEMENT = "INSERT INTO datasets (created_at, updated_at, external_id, internal_id) values (NOW(),NOW(),?,?)";
    private final static String SELECT_CAT_STATEMENT = "SELECT * FROM catalogues WHERE external_id=?";
    private final static String SELECT_DS_STATEMENT = "SELECT * FROM datasets WHERE external_id=?";

    public BrokerMessageController(Vertx vertx) {
        this.brokerMessageService = BrokerMessageService.createProxy(vertx,"brokerMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
    }

    public void getData (String input, Handler<AsyncResult<String>> readyHandler){
        ConnectorNotificationMessage header = IDSMessageParser.getHeader(input);
        Connector connector = IDSMessageParser.getBody(input);
        try {
            if (header instanceof ConnectorAvailableMessage) {
                LOGGER.info("AvailableMessage received.");
                register(connector, readyHandler);
            } else if (header instanceof ConnectorUnavailableMessage) {
                LOGGER.info("UnavailableMessage received.");
                unregister(connector, readyHandler);
            } else if (header instanceof ConnectorUpdateMessage) {
                LOGGER.info("UpdateMessage received.");
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

    private void update(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        Future<List<JsonObject>> catalogueIdFuture = Future.future();
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()), cataloguePersistenceReply -> catalogueIdFuture.completer());
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
                                    databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataassetId), datasetPersistenceReply -> {
                                        if (datasetPersistenceReply.succeeded()) {
                                            brokerMessageService.createDataSet(datassetFutures.get(dataassetId).result(), datasetPersistenceReply.result().get(0).getString("internal_id"), catalogueIdFuture.result().get(0).getString("internal_id"), datasetReply -> {
                                            });
                                        } else {

                                        }
                                    });
                                }
                               readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
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

    private void register(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        Future<String> catalogueFuture = Future.future();
        Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        catalogueFuture.setHandler( reply -> {
            if(reply.succeeded()) {
                String catalogueId = UUID.randomUUID().toString();
                brokerMessageService.createCatalogue(reply.result(), catalogueId, catalogueReply -> {
                    if (catalogueReply.succeeded()) {
                        databaseService.update(INSERT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()).add(catalogueId), cataloguePersistenceReply -> {});
                        CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler( dataassetCreateReply -> {
                            if(dataassetCreateReply.succeeded()){
                                for(String datasetExternalId : datassetFutures.keySet()){
                                    String datasetId = UUID.randomUUID().toString();
                                    brokerMessageService.createDataSet(datassetFutures.get(datasetExternalId).result(), datasetExternalId, catalogueId, datasetReply -> {});
                                    databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(Json.decodeValue(datassetFutures.get(datasetExternalId).result(), Resource.class).getId().toString()).add(datasetId), datasetPersistenceReply -> {});
                                }
                                readyHandler.handle(Future.succeededFuture("Connector successfully registered."));
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

    private void unregister(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()), cataloguePersistenceReply -> {
            if(cataloguePersistenceReply.succeeded()) {
                String catalogueInternalId = cataloguePersistenceReply.result().get(0).getString("internal_id");
                LOGGER.info("internal ID resolved: " + catalogueInternalId);
                List<Future> datasetDeleteFutures = new ArrayList<>();

                for (Resource dataasset : connector.getCatalog().getOffer()) {
                    databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataasset.getId().toString()), datasetIdreply -> {
                        if (datasetIdreply.succeeded() && !datasetIdreply.result().isEmpty()) {
                            Future datasetDeleteFuture = Future.future();
                            datasetDeleteFutures.add(datasetDeleteFuture);
                            String datasetInternalId = datasetIdreply.result().get(0).getString("internal_id");
                            brokerMessageService.deleteDataSet(datasetInternalId, catalogueInternalId, datasetDeleteFuture.completer());
                        } else {
                            LOGGER.error(datasetIdreply.cause());
                            readyHandler.handle(Future.failedFuture(datasetIdreply.cause()));
                        }
                    });
                }
                LOGGER.info("Datasets deleted: " +datasetDeleteFutures.size());
                CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
                    if (reply.succeeded()) {
                        brokerMessageService.deleteCatalogue(catalogueInternalId, datasetReply -> {
                        });
                        readyHandler.handle(Future.succeededFuture("Connector successfully unregistered."));
                    } else {
                        LOGGER.error(reply.cause());
                        readyHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                LOGGER.error(cataloguePersistenceReply.cause());
                readyHandler.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
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
