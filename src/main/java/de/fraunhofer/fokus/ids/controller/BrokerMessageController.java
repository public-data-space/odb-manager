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
    private static final String DELETE_DS_UPDATE = "DELETE FROM datasets WHERE internal_id = ?";
    private static final String DELETE_DS_BY_IDS_ID_UPDATE = "DELETE FROM datasets WHERE external_id = ?";

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

    private void update(Connector connector, Handler<AsyncResult<String>> readyHandler){
        resolveCatalogueId(connector.getId().toString(),next ->{
            if(next.succeeded()) {
                Future<String> catalogueFuture = Future.future();
                Map<String, Future<String>> datassetFutures = new HashMap<>();
                initTransformations(connector, catalogueFuture, datassetFutures);
                catalogueFuture.setHandler(reply -> {
                    handleCatalogueExternal(reply, next.result(), next2 -> {
                        updateDatasets(datassetFutures, next.result(), readyHandler);
                    });
                });
            } else {
                LOGGER.error(next.cause());
                readyHandler.handle(Future.failedFuture(next.cause()));
            }
        });
    }

    private void updateDatasets(Map<String, Future<String>> datassetFutures, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
       Set<String> messageDatasetIds = datassetFutures.keySet();

        dataAssetIdsOfCatalogue(catalogueId, piveauDatasetIds -> resolvePiveauIds(piveauDatasetIds,  result -> {
               if(result.succeeded()){
                   Map<String, Future> datasetUpdateFutures = new HashMap<>();
                   Set<String> availableDatasetIds = new HashSet(result.result().keySet());
                   availableDatasetIds.addAll(messageDatasetIds);
                   for(String messageDatasetId : availableDatasetIds){
                       datasetUpdateFutures.put(messageDatasetId, Future.future());
                   }
                    Collection<String> piveauIds = result.result().keySet();
                    if (piveauIds.isEmpty()&&messageDatasetIds.isEmpty()){
                        readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                    }
                    else{
                        for(String messageId : messageDatasetIds){
                            if(piveauIds.contains(messageId)){
                                updateDataset(datassetFutures, messageId, catalogueId, datasetUpdateFutures);
                            } else {
                                String internalId = UUID.randomUUID().toString();
                                createDataSet(datassetFutures, messageId, internalId, catalogueId, datasetUpdateFutures);
                            }
                            piveauIds.remove(messageId);
                        }
                        for(String orphan : piveauIds){
                            deleteDataseInPiveau(result.result().get(orphan), catalogueId, res -> deleteDatasetInDatabase(res, DELETE_DS_BY_IDS_ID_UPDATE, orphan, datasetUpdateFutures));
                        }
                        CompositeFuture.all(new ArrayList<>(datasetUpdateFutures.values())).setHandler(ac -> {
                            if(ac.succeeded()){
                                readyHandler.handle(Future.succeededFuture("Connector successfully updated."));
                            } else {
                                readyHandler.handle(Future.failedFuture(ac.cause()));
                            }
                        });
                    }
           } else {
               readyHandler.handle(Future.failedFuture(result.cause()));
           }
       }));

    }

    private void updateDataset(Map<String, Future<String>> datassetFutures, String messageId, String catalogueId, Map<String, Future> datasetUpdateFutures) {
        resolveDatasetsForUpdate(messageId, reply -> {
            brokerMessageService.createDataSet(datassetFutures.get(messageId).result(), reply.result(), catalogueId, datasetReply -> {
                if(datasetReply.succeeded()) {
                    datasetUpdateFutures.get(messageId).complete();
                }else {
                    LOGGER.error(datasetReply.cause());
                    datasetUpdateFutures.get(messageId).fail(datasetReply.cause());
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

    private void deleteDatasetInDatabase(AsyncResult<Void> reply, String update, String idsId, Map<String, Future> datasetdeleteFutures){
        if (reply.succeeded()){
            databaseService.update(update,new JsonArray().add(idsId),internalDatasetDeleteResult ->{
                if (reply.succeeded()) {
                    datasetdeleteFutures.get(idsId).complete();
                    LOGGER.info("DataAsset From Database successfully deleted");
                } else {
                    LOGGER.error(reply.cause());
                    datasetdeleteFutures.get(idsId).fail(reply.cause());
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

        private void createDataSet(Map<String, Future<String>> datassetFutures,String datasetExternalId,String dataSetId, String catalogueId, Map<String, Future> datasetUpdateFutures ) {
        brokerMessageService.createDataSet(datassetFutures.get(datasetExternalId).result(),dataSetId ,catalogueId , datasetReply -> {
            if(datasetReply.succeeded()){
                databaseService.update(INSERT_DS_STATEMENT, new JsonArray().add(datasetExternalId).add(dataSetId), datasetPersistenceReply2 -> {
                    if(datasetPersistenceReply2.succeeded()){
                        datasetUpdateFutures.get(datasetExternalId).complete();
                    } else {
                        datasetUpdateFutures.get(datasetExternalId).fail(datasetPersistenceReply2.cause());
                    }
                });
            } else {
                LOGGER.error(datasetReply.cause());
            }
        });
    }

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
                    Map<String, Future> dataassetCreateFutures = new HashMap<>();
                    for(String datasetExternalId : datassetFutures.keySet()){
                        dataassetCreateFutures.put(datasetExternalId, Future.future());
                        String datasetId = UUID.randomUUID().toString();
                        createDataSet(datassetFutures,datasetExternalId,datasetId,catalogueIdResult.result(), dataassetCreateFutures);
                    }
                    CompositeFuture.all(new ArrayList<>(dataassetCreateFutures.values())).setHandler( ac -> {
                        if(ac.succeeded()) {
                            readyHandler.handle(Future.succeededFuture("Connector successfully registered."));
                        } else {
                            LOGGER.error(ac.cause());
                            readyHandler.handle(Future.failedFuture(ac.cause()));
                        }
                    });
                } else {
                    LOGGER.error(dataassetCreateReply.cause());
                    readyHandler.handle(Future.failedFuture(dataassetCreateReply.cause()));
                }
            });
        } else {
            readyHandler.handle(Future.failedFuture(catalogueIdResult.cause()));
        }
    }

    private void unregister(Connector connector, Handler<AsyncResult<String>> readyHandler) {
        Map<String, Future> datasetDeleteFutures = new HashMap<>();

        resolveCatalogueId(connector.getId().toString(), catalogueIdResult -> {
            if(catalogueIdResult.succeeded()) {
                    dataAssetIdsOfCatalogue(catalogueIdResult.result(),piveauDatasetIds->{
                        if(piveauDatasetIds.succeeded()) {
                            if (!piveauDatasetIds.result().isEmpty()) {
                                for (String id : piveauDatasetIds.result()) {
                                    Future datasetDeleteFuture = Future.future();
                                    datasetDeleteFutures.put(id, datasetDeleteFuture);
                                    deleteDataseInPiveau(id, catalogueIdResult.result(), next -> deleteDatasetInDatabase(next, DELETE_DS_UPDATE, id, datasetDeleteFutures));
                                }
                            }
                            for (Resource dataasset : connector.getCatalog().getOffer()) {
                                Future datasetDeleteFuture = Future.future();
                                datasetDeleteFutures.put(dataasset.getId().toString(), datasetDeleteFuture);
                                databaseService.query(SELECT_DS_STATEMENT, new JsonArray().add(dataasset.getId().toString()), datasetIdreply -> {
                                    if(datasetIdreply.succeeded()) {
                                        if(!datasetIdreply.result().isEmpty()) {
                                            String datasePiveautId = datasetIdreply.result().get(0).getString("internal_id");
                                            String datasetIdsId = datasetIdreply.result().get(0).getString("external_id");
                                            deleteDatasetPiveau(datasePiveautId, catalogueIdResult, externalDeleteReply ->
                                                    deleteDatasetInternal(externalDeleteReply, datasePiveautId, datasetIdsId, datasetDeleteFutures));
                                        } else {
                                            datasetDeleteFuture.complete();
                                        }
                                    }
                                    else {
                                        LOGGER.error(datasetIdreply.cause());
                                        readyHandler.handle(Future.failedFuture(datasetIdreply.cause()));
                                    }
                                });
                            }
                            handleCatalogue(new ArrayList<>(datasetDeleteFutures.values()), catalogueIdResult.result(), readyHandler);
                        } else {
                            LOGGER.error(piveauDatasetIds.cause());
                            readyHandler.handle(Future.failedFuture(piveauDatasetIds.cause()));
                        }
                    });
            } else {
                readyHandler.handle(Future.failedFuture(catalogueIdResult.cause()));
            }
        });
    }

    private void handleCatalogue(List<Future> datasetDeleteFutures, String catalogueIdResult, Handler<AsyncResult<String>> readyHandler){
        CompositeFuture.all(datasetDeleteFutures).setHandler(reply -> {
            if (reply.succeeded()) {
                deleteCatalogueExternal(reply, catalogueIdResult, externalCatalogueDeleteReply ->
                        deleteCatalogueInternal(externalCatalogueDeleteReply, catalogueIdResult,readyHandler));
            } else {
                readyHandler.handle(Future.failedFuture(reply.cause()));
            }
        });
    }

    private void resolveCatalogueId(String connectorId, Handler<AsyncResult<String>> next) {
        databaseService.query(SELECT_CAT_STATEMENT, new JsonArray().add(connectorId), cataloguePersistenceReply -> {
            if (cataloguePersistenceReply.succeeded() && !cataloguePersistenceReply.result().isEmpty()) {
                LOGGER.info("internal ID resolved: " + cataloguePersistenceReply.result().get(0).getString("internal_id"));
                next.handle(Future.succeededFuture(cataloguePersistenceReply.result().get(0).getString("internal_id")));
            } else {
                LOGGER.error(cataloguePersistenceReply.cause());
                next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
            }
        });
    }

    private void deleteDatasetPiveau(String datasetId, AsyncResult<String> catalogueId, Handler<AsyncResult<Void>> next){
        brokerMessageService.deleteDataSet(datasetId, catalogueId.result(), deleteAsset ->{
            if (deleteAsset.succeeded()){
                next.handle(Future.succeededFuture());
            } else {
                LOGGER.error(deleteAsset.cause());
                next.handle(Future.failedFuture(deleteAsset.cause()));
            }
        });
    }

    private void deleteDatasetInternal(AsyncResult<Void> reply, String datasetPiveauId, String datasetIDSId, Map<String, Future> datasetDeleteFutures){
        if (reply.succeeded()){
            databaseService.update(DELETE_DS_UPDATE,new JsonArray().add(datasetPiveauId),internalDatasetDeleteResult ->{
                if (reply.succeeded()) {
                    LOGGER.info("DataAsset From Database successfully deleted");
                    datasetDeleteFutures.get(datasetIDSId).complete();
                } else {
                    datasetDeleteFutures.get(datasetIDSId).fail(reply.cause());
                    LOGGER.error(reply.cause());
                }
            });
        } else {
            datasetDeleteFutures.get(datasetPiveauId).fail(reply.cause());
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
