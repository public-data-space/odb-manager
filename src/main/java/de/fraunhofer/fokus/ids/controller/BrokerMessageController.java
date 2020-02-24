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
import java.util.Collection;
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
    private final static String RESOLVE_DS_STATEMENT = "SELECT * FROM datasets WHERE internal_id=?";
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
                update3(connector, readyHandler);
            } else {
                LOGGER.error("Invalid message signature.");
            }
        }
        catch (Exception e){
            e.printStackTrace();
            LOGGER.error("Something went wrong while parsing the IDS message.");
        }
    }

//    private void update(Connector connector, Handler<AsyncResult<String>> readyHandler) {
//        Future<List<JsonObject>> catalogueIdFuture = Future.future();
//        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connector.getId().toString()), cataloguePersistenceReply -> catalogueIdFuture.completer());
//        Future<String> catalogueFuture = Future.future();
//        Map<String, Future<String>> datassetFutures = new HashMap<>();
//        initTransformations(connector, catalogueFuture, datassetFutures);
//
//        CompositeFuture.all(catalogueFuture,catalogueIdFuture).setHandler( reply -> {
//            if(reply.succeeded()) {
//                brokerMessageService.createCatalogue(catalogueFuture.result(), catalogueIdFuture.result().get(0).getString("internal_id"), catalogueReply -> {
//                    if (catalogueReply.succeeded()) {
//                        CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler(dataassetCreateReply -> {
//                           if(dataassetCreateReply.succeeded()){
//                                for(String dataassetId: datassetFutures.keySet()) {
//                                    databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataassetId), datasetPersistenceReply -> {
//                                        if (datasetPersistenceReply.succeeded()) {
//                                            brokerMessageService.createDataSet(datassetFutures.get(dataassetId).result(), datasetPersistenceReply.result().get(0).getString("internal_id"), catalogueIdFuture.result().get(0).getString("internal_id"), datasetReply -> {
//                                                if(datasetReply.succeeded()){
//                                                    databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(dataassetId).add(datasetPersistenceReply.result().get(0).getString("internal_id")), datasetPersistenceReply2 -> {});
//                                                } else {
//                                                    LOGGER.error(datasetReply.cause());
//                                                }
//                                            });
//                                        } else {
//
//                                        }
//                                    });
//                                }
//                               readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
//                           } else {
//                            LOGGER.error(dataassetCreateReply.cause());
//                            readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
//                           }
//                        });
//                    } else {
//                        LOGGER.error(catalogueReply.cause());
//                        readyHandler.handle(Future.failedFuture(catalogueReply.cause()));
//
//                    }
//                });
//            } else {
//                LOGGER.error(reply.cause());
//                readyHandler.handle(Future.failedFuture(reply.cause()));
//            }
//        });
//    }


    private void update3(Connector connector, Handler<AsyncResult<String>> readyHandler){
        resolveCatalogueId(connector.getId().toString(),next ->{
            Future<String> catalogueFuture = Future.future();
            Map<String, Future<String>> datassetFutures = new HashMap<>();
            initTransformations(connector, catalogueFuture, datassetFutures);
            catalogueFuture.setHandler(reply -> {
                handleCatalogueExternal(reply,next.result(),next2->{
                    updateDatasets2(datassetFutures,next.result(),readyHandler);
                });
            });
        });
    }

    private void updateDatasets2( Map<String, Future<String>> datassetFutures, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
       Future<Map<String, String>> piveauDatasetExternalIds = Future.future();
       dataAssetIdsOfCatalogue(catalogueId, piveauDatasetIds -> resolvePiveauIds(piveauDatasetIds, piveauDatasetExternalIds.completer()));
       Set<String> messageDatasetIds = datassetFutures.keySet();

       piveauDatasetExternalIds.setHandler( result -> {
           if(result.succeeded()){
               Collection<String> piveauIds = result.result().keySet();
                for(String messageId : messageDatasetIds){
                    if(piveauIds.contains(messageId)){
                        updateDataset(datassetFutures, messageId, catalogueId, readyHandler);
                    } else {
                        String internalId = UUID.randomUUID().toString();
                        createDataSet(datassetFutures, messageId, internalId, catalogueId);
                        readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                    }
                    piveauIds.remove(messageId);
                }
                for(String orphan : piveauIds){
                    deleteDataseInPiveau(result.result().get(orphan), catalogueId, res -> deleteDatasetInDatabase(res, orphan, readyHandler));
                }

           } else {
               readyHandler.handle(Future.failedFuture(result.cause()));
           }
       });

    }

    private void updateDataset(Map<String, Future<String>> datassetFutures, String messageId, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        resolveDatasetsForUpdate(messageId, reply -> {
            brokerMessageService.createDataSet(datassetFutures.get(messageId).result(), reply.result(), catalogueId, datasetReply -> {
                if(datasetReply.succeeded()) {
                    readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                }else {
                    LOGGER.error(datasetReply.cause());
                    readyHandler.handle(Future.failedFuture(datasetReply.cause()));
                }
            });
        });
    }

    private void resolveDatasetsForUpdate(String dataassetIdExternal, Handler<AsyncResult<String>> next ){
        databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataassetIdExternal), datasetPersistenceReply -> {
            if (datasetPersistenceReply.succeeded()) {
                if (!datasetPersistenceReply.result().isEmpty()) {
                    String id = datasetPersistenceReply.result().get(0).getString("internal_id");
                    next.handle(Future.succeededFuture(id));
                }
                else {
                    String datId = UUID.randomUUID().toString();
                    next.handle(Future.succeededFuture(datId));
                }
            }
            else {
                LOGGER.error(datasetPersistenceReply.cause());
                next.handle(Future.failedFuture(datasetPersistenceReply.cause()));
            }
        });
    }

    private void deleteDataseInPiveau(String datasetId, String catalogueId, Handler<AsyncResult<Void>> next) {
        brokerMessageService.deleteDataSet(datasetId, catalogueId, deleteAsset -> {
            if (deleteAsset.succeeded()) {
                next.handle(Future.succeededFuture());
            } else {
                LOGGER.error(deleteAsset.cause());
                next.handle(Future.failedFuture(deleteAsset.cause()));
            }
        });
    }

    private void deleteDatasetInDatabase(AsyncResult<Void> reply, String datasetInternalId, Handler<AsyncResult<String>> readyHandler){
        if (reply.succeeded()){
            databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(datasetInternalId),internalDatasetDeleteResult ->{
                if (reply.succeeded()) {
                    readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                    LOGGER.info("DataAsset From Database successfully deleted");
                } else {
                    LOGGER.error(reply.cause());
                    readyHandler.handle(Future.failedFuture(reply.cause()));
                }
            });
        } else {
            LOGGER.error("DataAsset delete failed");
        }
    }

    private void resolvePiveauIds(AsyncResult<List<String>> piveauDatasetIds, Handler<AsyncResult<Map<String, String>>> completer) {
        Map<String, Future<List<JsonObject>>> piveau2IDSResolveFutureMap = new HashMap<>();
        if(piveauDatasetIds.succeeded()) {
            for (String piveauId : piveauDatasetIds.result()){
                Future idsResolve = Future.future();
                piveau2IDSResolveFutureMap.put(piveauId, idsResolve);
                databaseService.query(RESOLVE_DS_STATEMENT, new JsonArray().add(piveauId), idsResolve.completer());
            }
            CompositeFuture.all(new ArrayList<>(piveau2IDSResolveFutureMap.values())).setHandler( ac -> {
                if(ac.succeeded()){
                    Map<String, String> resultMap = new HashMap<>();
                    for(String piveauId : piveau2IDSResolveFutureMap.keySet()){
                        resultMap.put(piveau2IDSResolveFutureMap.get(piveauId).result().get(0).getString("external_id"), piveauId);
                    }
                    completer.handle(Future.succeededFuture(resultMap));
                } else {
                    completer.handle(Future.failedFuture(ac.cause()));
                }
            });
        } else {
            completer.handle(Future.failedFuture(piveauDatasetIds.cause()));
        }

    }


//    private void update2(Connector connector, Handler<AsyncResult<String>> readyHandler) {
//        resolveCatalogueId(connector.getId().toString(),next ->{
//            Future<String> catalogueFuture = Future.future();
//            Map<String, Future<String>> datassetFutures = new HashMap<>();
//            initTransformations(connector, catalogueFuture, datassetFutures);
//            catalogueFuture.setHandler(reply -> {
//                handleCatalogueExternal(reply,next.result(),next2->{
//                    updateDatasets(next,datassetFutures,next.result(),readyHandler);
//                });
//            });
//        });
//        }

//    private void updateDatasets(AsyncResult<String> catalogueIdResult, Map<String, Future<String>> datassetFutures,String catalogueInternalId ,Handler<AsyncResult<String>> readyHandler){
//        if (catalogueIdResult.succeeded()) {
//            CompositeFuture.all(new ArrayList<>(datassetFutures.values())).setHandler( dataassetCreateReply -> {
//                if(dataassetCreateReply.succeeded()){
//                    if (datassetFutures.isEmpty()){
//                        deleteDatasetsForUpdate(catalogueInternalId,readyHandler);
//                    }
//                    else{
//                        handleUpdateDatasets(datassetFutures,catalogueInternalId,readyHandler);
//                    }
//                } else {
//                    LOGGER.error(dataassetCreateReply.cause());
//                    readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
//                }
//            });
//        } else {
//            readyHandler.handle(Future.failedFuture(catalogueIdResult.cause()));
//        }
//    }

//    public void handleUpdateDatasets(Map<String, Future<String>> datassetFutures,String catalogueInternalId,Handler<AsyncResult<String>> readyHandler){
//        handleIdsFromPiveau(catalogueInternalId,arrayListAsyncResult -> {
//            if (arrayListAsyncResult.result()!=null){
//                handleWithTheReturnedMapIds(false,datassetFutures,catalogueInternalId,arrayListAsyncResult.result(),readyHandler);
//            }
//            else {
//                handleWithTheReturnedMapIds(true,datassetFutures,catalogueInternalId,arrayListAsyncResult.result(),readyHandler);
//            }
//        });
//    }
//
//    private void deleteDatasetsForUpdate(String catalogueInternalId, Handler<AsyncResult<String>> readyHandler){
//        dataAssetIdsOfCatalogue(catalogueInternalId,arrayListAsyncResult -> {
//            if (arrayListAsyncResult.succeeded()){
//                for (String dataAssetId : arrayListAsyncResult.result()){
//                    brokerMessageService.deleteDataSet(dataAssetId,catalogueInternalId,deleteHandler->{
//                        if (deleteHandler.succeeded()){
//                            databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(dataAssetId),r2->{});
//                            LOGGER.info("Update succeeded");
//                            readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
//                        }
//                        else{
//                            LOGGER.error(deleteHandler.cause());
//                            readyHandler.handle(Future.failedFuture(deleteHandler.cause()));
//                        }
//                    });
//                }
//            }
//            else {
//                LOGGER.error(arrayListAsyncResult.cause());
//                readyHandler.handle(Future.failedFuture(arrayListAsyncResult.cause()));
//            }
//        });
//    }

//    private void resolveAllDatasetIdsFromMessage (Map<String, Future<String>> datassetFutures,Handler<AsyncResult<List<?>>> resultHandler){
//        List<Future> mapFutures = new ArrayList<>();
//        for (String externalId : datassetFutures.keySet()){
//            Future<Map<String,String>> map = getIdsFromMessage(externalId);
//            mapFutures.add(map);
//        }
//        CompositeFuture.all(mapFutures).setHandler(reply->{
//            resultHandler.handle(Future.succeededFuture(reply.result().list()));
//        });
//
//    }

//    private Future<Map<String,String>> getIdsFromMessage(String dataassetIdExternal) {
//        Future<Map<String,String>> future = Future.future();
//        resolveDatasetsForUpdate(dataassetIdExternal,next->{
//            if (next.succeeded()){
//                Map<String , String> map = new HashMap<>();
//                map.put("internalId",next.result());
//                map.put("externalId",dataassetIdExternal);
//                future.complete(map);
//            }
//        });
//        return future;
//    }

//    private void handleIdsFromPiveau(String catalogueInternalId,Handler<AsyncResult<ArrayList<String>>> resultHandler){
//        dataAssetIdsOfCatalogue(catalogueInternalId,r -> {
//            if (r.succeeded()) {
//                resultHandler.handle(Future.succeededFuture(r.result()));
//            }
//            else {
//                LOGGER.error(r.cause());
//            }
//        });
//    }
//    private void returnMapOfIds(Map<String, Future<String>> datassetFutures,Handler<AsyncResult<ArrayList<Map<String,String>>>> resultHandler){
//        resolveAllDatasetIdsFromMessage(datassetFutures,asyncResult->{
//            if (asyncResult.succeeded()){
//                ArrayList<Map<String,String>> result = (ArrayList<Map<String, String>>) asyncResult.result();
//                resultHandler.handle(Future.succeededFuture(result));
//            }
//            else {
//                LOGGER.error(asyncResult.cause());
//            }
//        });
//    }

//    private void handleWithTheReturnedMapIds(boolean checkNull ,Map<String, Future<String>> datassetFutures,String catalogueInternalId,ArrayList<String> listIdsFromPiveau,Handler<AsyncResult<String>> readyHandler){
//        returnMapOfIds(datassetFutures,arrayListAsyncResult -> {
//            if (arrayListAsyncResult.succeeded()){
//                if (checkNull){
//                    for (Map<String,String> map:arrayListAsyncResult.result()){
//                        createDataSet(datassetFutures,map.get("externalId"),map.get("internalId"),catalogueInternalId);
//                    }
//                    readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
//                }
//                else {
//                    ArrayList<String> internalIdList = new ArrayList<>();
//
//                    for (Map<String,String> map:arrayListAsyncResult.result()){
//                        internalIdList.add(map.get("internalId"));
//                        String internalId = map.get("internalId");
//                        String externalId = map.get("externalId");
//                        if (listIdsFromPiveau.contains(internalId)){
//                            databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(internalId),deleteReply->{});
//                            createDataSet(datassetFutures,externalId,internalId,catalogueInternalId);
//                        }
//                        else{
//                            createDataSet(datassetFutures,externalId,internalId,catalogueInternalId);
//                        }
//                    }
//
//                    for (String id:listIdsFromPiveau){
//                        if (!internalIdList.contains(id)){
//                            brokerMessageService.deleteDataSet(id,catalogueInternalId,brokerMessageServiceAsyncResult -> {});
//                        }
//                    }
//                    readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
//                }
//            }
//            else {
//                LOGGER.error(arrayListAsyncResult.cause());
//            }
//        });
//    }

    private void dataAssetIdsOfCatalogue (String catalogueInternalId, Handler<AsyncResult<List<String>>> asyncResultHandler){
            brokerMessageService.getAllDatasetsOfCatalogue(catalogueInternalId,jsonReply ->{
                if (jsonReply.succeeded()) {
                    ArrayList<String> ids = new ArrayList<>();
                    if (!jsonReply.result().isEmpty()){
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

        private void createDataSet(Map<String, Future<String>> datassetFutures,String datasetExternalId,String dataSetId, String catalogueId ) {
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
        String catalogueId = UUID.randomUUID().toString();
        Future<String> catalogueFuture = Future.future();
        Map<String, Future<String>> datassetFutures = new HashMap<>();
        initTransformations(connector, catalogueFuture, datassetFutures);
        catalogueFuture.setHandler( catalogueTTLResult ->
                handleCatalogueExternal(catalogueTTLResult,catalogueId ,piveauCatalogueReply ->
                        handleCatalogueInternal(piveauCatalogueReply, connector.getId().toString(), internalCatalogueReply ->
                                handleDatasets(internalCatalogueReply, datassetFutures, readyHandler))));
    }

    private void handleCatalogueExternal(AsyncResult<String> catalogue,String catalogueId, Handler<AsyncResult<String>> next){
        if(catalogue.succeeded()) {
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
