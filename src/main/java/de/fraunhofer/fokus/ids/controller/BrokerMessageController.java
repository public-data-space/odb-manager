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
    private final static String SELECT_DS_WITH_INTERNALID_STATEMENT = "SELECT * FROM datasets WHERE internal_id=?";
    private static final String DELETE_DS_UPDATE = "DELETE FROM datasets WHERE internal_id = ?";
    private final static String DELETE_CAT_STATEMENT = "DELETE FROM catalogues WHERE internal_id = ?";

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
                update2(connector, readyHandler);
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
        Map<String, Future<String>> datassetFutures = new HashMap<>();
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
                                                if(datasetReply.succeeded()){
                                                    databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(dataassetId).add(datasetPersistenceReply.result().get(0).getString("internal_id")), datasetPersistenceReply2 -> {});
                                                } else {
                                                    LOGGER.error(datasetReply.cause());
                                                }
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

    private void update2(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()), cataloguePersistenceReply -> {
            if (cataloguePersistenceReply.succeeded()){
                String catalogueInternalId = cataloguePersistenceReply.result().get(0).getString("internal_id");
                LOGGER.info("internal ID resolved: " + catalogueInternalId);

                Future<String> catalogueFuture = Future.future();
                Map<String, Future<String>> datassetFutures = new HashMap<>();
                initTransformations(connector, catalogueFuture, datassetFutures);
                catalogueFuture.setHandler(reply -> {
                    if (reply.succeeded()){
                        brokerMessageService.createCatalogue(catalogueFuture.result(), catalogueInternalId, catalogueReply -> {
                            if (catalogueReply.succeeded()) {
                                CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler(dataassetCreateReply -> {
                                    if(dataassetCreateReply.succeeded()){
                                        if (datassetFutures.isEmpty()){
                                            dataAssetIdsOfCatalogue(catalogueInternalId,arrayListAsyncResult -> {
                                                if (arrayListAsyncResult.succeeded()){
                                                    for (String dataAssetId : arrayListAsyncResult.result()){
                                                        brokerMessageService.deleteDataSet(dataAssetId,catalogueInternalId,deleteHandler->{
                                                            if (deleteHandler.succeeded()){
                                                                LOGGER.info("Update succeeded");
                                                            }
                                                            else{
                                                                LOGGER.error("Delete failure!");
                                                            }
                                                        });
                                                    }
                                                }
                                            });
                                            readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                                        }
                                        else{
                                            for(String dataassetIdExternal: datassetFutures.keySet()) {
                                                databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataassetIdExternal), datasetPersistenceReply -> {
                                                    if (datasetPersistenceReply.succeeded()) {
                                                        if (!datasetPersistenceReply.result().isEmpty()) {
                                                            dataAssetIdsOfCatalogue(catalogueInternalId,r -> {
                                                                if (r.succeeded()){
                                                                    if (r.result()!=null){
                                                                        String id = datasetPersistenceReply.result().get(0).getString("internal_id");
                                                                        ArrayList<String> listIds = r.result();
                                                                        if (!listIds.contains(id)){
                                                                            createDataSet(datassetFutures,dataassetIdExternal,id,catalogueInternalId);
                                                                        }
                                                                      for (String s : listIds){
                                                                          databaseService.query(SELECT_DS_WITH_INTERNALID_STATEMENT,new JsonArray().add(s),externalIdreply -> {
                                                                              for (JsonObject extId : externalIdreply.result()){
                                                                                if (extId.getString("external_id").equals(dataassetIdExternal)){
                                                                                    if (s.equals(id)){
                                                                                        databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(id),r2->{});
                                                                                        createDataSet(datassetFutures,dataassetIdExternal,id,catalogueInternalId);
                                                                                    }
                                                                                }
                                                                                else {
                                                                                    brokerMessageService.deleteDataSet(s,catalogueInternalId,brokerMessageServiceAsyncResult -> {});
                                                                                }
                                                                              }
                                                                          });

                                                                      }

                                                                    }
                                                                    else {
                                                                        String datasetId = UUID.randomUUID().toString();
                                                                        createDataSet(datassetFutures,dataassetIdExternal,datasetId,catalogueInternalId);
                                                                    }
                                                                }
                                                                else {
                                                                 LOGGER.error(r.cause());
                                                                }
                                                            });
                                                        }
                                                        else{
                                                            String datId = UUID.randomUUID().toString();
                                                            createDataSet(datassetFutures,dataassetIdExternal,datId,catalogueInternalId);
                                                        }
                                                    } else {
                                                        LOGGER.error(datasetPersistenceReply.cause());
                                                    }
                                                });
                                            }
                                            readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                                        }

                                    }
                                    else {
                                        LOGGER.error(dataassetCreateReply.cause());
                                        readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
                                    }
                                });
                            }
                            else {
                                LOGGER.error(catalogueReply.cause());
                                readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
                            }
                        });
                    }
                    else {
                        LOGGER.error(reply.cause());
                        readyHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            }
            else{
                LOGGER.error(cataloguePersistenceReply.cause());
            }
            });
        }

        private void dataAssetIdsOfCatalogue (String catalogueInternalId, Handler<AsyncResult<ArrayList<String>>> asyncResultHandler){
            brokerMessageService.getAllDatasetsOfCatalogue(catalogueInternalId,jsonReply ->{
                if (jsonReply.succeeded()) {
                    ArrayList<String> ids = new ArrayList<>();
                    if (jsonReply.result().isEmpty()){
                        ids = null;
                    }
                    else {
                        for (Object jsonObject:jsonReply.result().getJsonArray("@graph")) {
                            JsonObject dataAsset = (JsonObject) jsonObject;
                            String idString = dataAsset.getString("@id");
                            String containsString = "https://ids.fokus.fraunhofer.de/set/data/";
                            if (idString.toLowerCase().contains(containsString.toLowerCase())){
                                String dataAssetId = idString.substring(containsString.length());
                                ids.add(dataAssetId);
                            }
                        }
                    }

                    asyncResultHandler.handle(Future.succeededFuture(ids));

                }
                else {
                    LOGGER.error("Can not get Ids of Catalogue");
                    asyncResultHandler.handle(Future.failedFuture(jsonReply.cause()));

                }
            });
        }
        private void createDataSet(Map<String, Future<String>> datassetFutures,String datasetExternalId,String dataSetId ,  String catalogueId ) {
        brokerMessageService.createDataSet(datassetFutures.get(datasetExternalId).result(),dataSetId ,catalogueId , datasetReply -> {
            if(datasetReply.succeeded()){
                databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(datasetExternalId).add(dataSetId), datasetPersistenceReply2 -> {});
            } else {
                LOGGER.error(datasetReply.cause());
            }
        });
    }

//    private void register2(Connector connector, Handler<AsyncResult<String>> readyHandler) {
//        Future<String> catalogueFuture = Future.future();
//        Map<String, Future<String>> datassetFutures = new HashMap<>();
//        initTransformations(connector, catalogueFuture, datassetFutures);
//
//        catalogueFuture.setHandler( reply -> {
//            if(reply.succeeded()) {
//                String catalogueId = UUID.randomUUID().toString();
//                brokerMessageService.createCatalogue(reply.result(), catalogueId, catalogueReply -> {
//                    if (catalogueReply.succeeded()) {
//                        databaseService.update(INSERT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()).add(catalogueId), cataloguePersistenceReply -> {});
//                        CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler( dataassetCreateReply -> {
//                            if(dataassetCreateReply.succeeded()){
//                                for(String datasetExternalId : datassetFutures.keySet()){
//                                    String datasetId = UUID.randomUUID().toString();
//                                    createDataSet(datassetFutures,datasetExternalId,datasetId,catalogueId);
//                                }
//                                readyHandler.handle(Future.succeededFuture("Connector successfully registered."));
//                            } else {
//                                LOGGER.error(dataassetCreateReply.cause());
//                                readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
//                            }
//                        });
//                    } else {
//                        LOGGER.error(catalogueReply.cause());
//                        readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
//                    }
//                });
//            } else {
//                LOGGER.error(reply.cause());
//                readyHandler.handle(Future.failedFuture(reply.cause()));
//            }
//        });
//    }

    private void register(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        Future<String> catalogueFuture = Future.future();
        Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);

        catalogueFuture.setHandler( catalogueTTLResult ->
                handleCatalogueExternal(catalogueTTLResult, piveauCatalogueReply ->
                        handleCatalogueInternal(piveauCatalogueReply, connector.getId().toString(), internalCatalogueReply ->
                                handleDatasets(internalCatalogueReply, datassetFutures, readyHandler))));
    }

    private void handleCatalogueExternal(AsyncResult<String> catalogue, Handler<AsyncResult<String>> next){
        if(catalogue.succeeded()) {
            String catalogueId = UUID.randomUUID().toString();
            brokerMessageService.createCatalogue(catalogue.result(), catalogueId, catalogueReply -> {
                if(catalogueReply.succeeded()) {
                    next.handle(Future.succeededFuture(catalogueId));
                }
                else {
                    LOGGER.error(catalogueReply.cause());
                    next.handle(Future.failedFuture(catalogueReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(catalogue.cause()));
        }
    }

    private void handleCatalogueInternal(AsyncResult<String> reply, String connectorId, Handler<AsyncResult<String>> next){
        if(reply.succeeded()) {
            databaseService.update(INSERT_CAT_STATEMENT, new JsonArray().add(connectorId).add(reply.result()), cataloguePersistenceReply -> {
                    if(cataloguePersistenceReply.succeeded()){
                        next.handle(Future.succeededFuture(reply.result()));
                    } else {
                        LOGGER.error(cataloguePersistenceReply.cause());
                        next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
                    }
            });
        }
        else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }

    private void handleDatasets(AsyncResult<String> catalogueIdResult, Map<String, Future<String>> datassetFutures, Handler<AsyncResult<String>> readyHandler){
        if (catalogueIdResult.succeeded()) {
            CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler( dataassetCreateReply -> {
                if(dataassetCreateReply.succeeded()){
                    for(String datasetExternalId : datassetFutures.keySet()){
                        String datasetId = UUID.randomUUID().toString();
                        createDataSet(datassetFutures,datasetExternalId,datasetId,catalogueIdResult.result());
                    }
                    readyHandler.handle(Future.succeededFuture("Connector successfully registered."));
                } else {
                    LOGGER.error(dataassetCreateReply.cause());
                    readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
                }
            });
        } else {
            readyHandler.handle(Future.failedFuture(catalogueIdResult.cause()));
        }
    }



//    private void unregister2(Connector connector, Handler<AsyncResult<String>> readyHandler) {
//        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()), cataloguePersistenceReply -> {
//            if(cataloguePersistenceReply.succeeded()) {
//                String catalogueInternalId = cataloguePersistenceReply.result().get(0).getString("internal_id");
//                LOGGER.info("internal ID resolved: " + catalogueInternalId);
//                List<Future> datasetDeleteFutures = new ArrayList<>();
//
//                for (Resource dataasset : connector.getCatalog().getOffer()) {
//                    databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataasset.getId().toString()), datasetIdreply -> {
//                        if (datasetIdreply.succeeded() && !datasetIdreply.result().isEmpty()) {
//                            Future datasetDeleteFuture = Future.future();
//                            datasetDeleteFutures.add(datasetDeleteFuture);
//                            String datasetInternalId = datasetIdreply.result().get(0).getString("internal_id");
//                            brokerMessageService.deleteDataSet(datasetInternalId, catalogueInternalId, deleteAsset ->{
//                                if (deleteAsset.succeeded()){
//                                    databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(datasetInternalId),reply ->{
//                                        if (reply.failed()) {
//                                            LOGGER.error(reply.cause());
//                                        } else {
//                                            LOGGER.info("DataAsset From Database succeeded deleted");
//                                        }
//                                    });
//                                }
//                            });
//                        } else {
//                            LOGGER.error(datasetIdreply.cause());
//                            readyHandler.handle(Future.failedFuture(datasetIdreply.cause()));
//                        }
//                    });
//                }
//                LOGGER.info("Datasets deleted: " +datasetDeleteFutures.size());
//                CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
//                    if (reply.succeeded()) {
//                        brokerMessageService.deleteCatalogue(catalogueInternalId, datasetReply -> {
//                            if (datasetReply.succeeded()){
//                                databaseService.update(DELETE_CAT_STATEMENT,new JsonArray().add(catalogueInternalId),reply2 ->{
//                                    if (reply2.failed()) {
//                                        LOGGER.error(reply2.cause());
//                                    } else {
//                                        LOGGER.info("Catalogue From Database succeeded deleted");
//                                    }
//                                });
//                            }
//                            else{
//                                LOGGER.error(datasetReply.cause());
//                            }
//                        });
//                        readyHandler.handle(Future.succeededFuture("Connector successfully unregistered."));
//                    } else {
//                        LOGGER.error(reply.cause());
//                        readyHandler.handle(Future.failedFuture(reply.cause()));
//                    }
//                });
//            } else {
//                LOGGER.error(cataloguePersistenceReply.cause());
//                readyHandler.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
//            }
//    });
//    }

    private void unregister(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        List<Future> datasetDeleteFutures = new ArrayList<>();

        resolveCatalogueId(connector.getId().toString(), catalogueIdResult -> {
            if(catalogueIdResult.succeeded()) {
                resolveDatasets(catalogueIdResult, connector.getCatalog().getOffer(), datasetId ->
                        deleteDatasetExternal(datasetId, catalogueIdResult, externalDeleteReply ->
                                deleteDatasetInternal(externalDeleteReply, datasetId, datasetDeleteFutures)));

                CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
                    if (reply.succeeded()) {
                        deleteCatalogueExternal(reply, catalogueIdResult.result(), externalCatalogueDeleteReply ->
                                deleteCatalogueInternal(externalCatalogueDeleteReply, catalogueIdResult.result(),readyHandler));
                    } else {
                        readyHandler.handle(Future.failedFuture(reply.cause()));
                    }
                });
            } else {
                readyHandler.handle(Future.failedFuture(catalogueIdResult.cause()));
            }
        });
    }


    private void resolveCatalogueId(String connectorId, Handler<AsyncResult<String>> next) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connectorId), cataloguePersistenceReply -> {
            if (cataloguePersistenceReply.succeeded()) {
                LOGGER.info("internal ID resolved: " + cataloguePersistenceReply.result().get(0).getString("internal_id"));
                next.handle(Future.succeededFuture(cataloguePersistenceReply.result().get(0).getString("internal_id")));
            } else {
                LOGGER.error(cataloguePersistenceReply.cause());
                next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
            }
        });
    }

    private void resolveDatasets(AsyncResult<String> catalogueId, List<? extends  Resource> dataassets, Handler<AsyncResult> next ){
        if(catalogueId.succeeded()) {
            for (Resource dataasset : dataassets) {
                databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataasset.getId().toString()), datasetIdreply -> {
                    if(datasetIdreply.succeeded() && !datasetIdreply.result().isEmpty()) {
                        next.handle(Future.succeededFuture(datasetIdreply.result().get(0).getString("internal_id")));
                    }
                    else {
                        LOGGER.error(datasetIdreply.cause());
                        next.handle(Future.failedFuture(datasetIdreply.cause()));
                    }
                });
            }
        } else {
            next.handle(Future.failedFuture(catalogueId.cause()));
        }
    }

    private void deleteDatasetExternal(AsyncResult<String> datasetId, AsyncResult<String> catalogueId, Handler<AsyncResult<Void>> next){
        if (datasetId.succeeded()) {
            brokerMessageService.deleteDataSet(datasetId.result(), catalogueId.result(), deleteAsset ->{
                if (deleteAsset.succeeded()){
                    next.handle(Future.succeededFuture());
                } else {
                    LOGGER.error(deleteAsset.cause());
                    next.handle(Future.failedFuture(deleteAsset.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(datasetId.cause()));
        }
    }

    private void deleteDatasetInternal(AsyncResult<Void> reply, AsyncResult<String> datasetInternalId, List<Future> datasetDeleteFutures){
        Future datasetDeleteFuture = Future.future();
        datasetDeleteFutures.add(datasetDeleteFuture);
        if (reply.succeeded()){
            databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(datasetInternalId),internalDatasetDeleteResult ->{
                if (reply.succeeded()) {
                    LOGGER.info("DataAsset From Database successfully deleted");
                    datasetDeleteFuture.complete();
                } else {
                    datasetDeleteFuture.fail(reply.cause());
                    LOGGER.error(reply.cause());
                }
            });
        } else {
            datasetDeleteFuture.fail(reply.cause());
        }
    }

    private void deleteCatalogueExternal(AsyncResult<CompositeFuture> reply, String catalogueInternalId, Handler<AsyncResult> next){
        if (reply.succeeded()) {
            brokerMessageService.deleteCatalogue(catalogueInternalId, deleteCatalogueReply -> {
                if(deleteCatalogueReply.succeeded()){
                    next.handle(Future.succeededFuture());
                } else {
                    LOGGER.error(deleteCatalogueReply.cause());
                    next.handle(Future.failedFuture(deleteCatalogueReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }

    private void deleteCatalogueInternal(AsyncResult<Void> reply, String catalogueInternalId, Handler<AsyncResult<String>> readyHandler){
        if (reply.succeeded()){
            databaseService.update(DELETE_CAT_STATEMENT,new JsonArray().add(catalogueInternalId),deleteCatalogueReply ->{
                if (deleteCatalogueReply.succeeded()) {
                    LOGGER.info("Catalogue From Database succeeded deleted");
                    readyHandler.handle(Future.succeededFuture("Connector successfully unregistered."));
                } else {
                    LOGGER.error(deleteCatalogueReply.cause());
                    readyHandler.handle(Future.failedFuture(deleteCatalogueReply.cause()));
                }
            });
        }
        else{
            LOGGER.error(reply.cause());
            readyHandler.handle(Future.failedFuture(reply.cause()));
        }
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
