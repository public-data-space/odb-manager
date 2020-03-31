package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.manager.DatasetManager;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.Connector;
import de.fraunhofer.iais.eis.RejectionReason;
import de.fraunhofer.iais.eis.Resource;
import de.fraunhofer.iais.eis.StaticEndpoint;
import de.fraunhofer.iais.eis.ids.jsonld.Serializer;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class UpdateController {
    private Logger LOGGER = LoggerFactory.getLogger(UpdateController.class.getName());

    private GraphManager graphManager;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;
    private IDSService idsService;
    private PiveauMessageService piveauMessageService;
    private DCATTransformerService dcatTransformerService;
    private Serializer serializer = new Serializer();

    public UpdateController(Vertx vertx, GraphManager graphManager, TSConnector tsConnector){
        this.graphManager = graphManager;
        this.catalogueManager = new CatalogueManager(vertx);
        this.datasetManager = new DatasetManager(vertx);
        this.idsService = new IDSService(vertx,tsConnector);
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, PiveauMessageService.ADDRESS);
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, DCATTransformerService.ADDRESS);
    }

    public void update(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {

        catalogueManager.getCatalogueByExternalId(connector.getId().toString(), catalogueIdResult -> {
            if (catalogueIdResult.succeeded()) {
                try {
                    graphManager.update(connector.getId().toString(),serializer.serialize(connector),next1->
                    {
                        try {
                            dcatTransformerService.transformCatalogue(serializer.serialize(connector), null, catalogueTransformationResult -> {
                                createCatalogueInPiveau(catalogueTransformationResult, catalogueIdResult.result().getString("internal_id"), next2 -> {
                                    updateDatasets(connector,uri, catalogueIdResult.result().getString("internal_id"), readyHandler);
                                });
                        });
                        } catch (IOException e) {
                            LOGGER.error(e);
                            readyHandler.handle(Future.failedFuture(e));
                        }
                    });
                } catch (IOException e) {
                    LOGGER.error(e);
                    readyHandler.handle(Future.failedFuture(e));
                }
            } else {
                LOGGER.error(catalogueIdResult.cause());
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
            }
        });
    }

        private void createCatalogueInPiveau(AsyncResult<String> catalogue, String catalogueId, Handler<AsyncResult<String>> next) {
        if (catalogue.succeeded()) {
            piveauMessageService.createCatalogue(catalogue.result(), catalogueId, catalogueReply -> {
                if (catalogueReply.succeeded()) {
                    next.handle(Future.succeededFuture(catalogueId));
                } else {
                    LOGGER.error(catalogueReply.cause());
                    next.handle(Future.failedFuture(catalogueReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(catalogue.cause()));
        }
    }

    private void updateDatasets(Connector connector, URI uri, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        datasetManager.dataAssetIdsOfCatalogue(catalogueId, piveauDatasetIds ->
                resolvePiveauIds(piveauDatasetIds, result -> {
            if (result.succeeded()) {
                java.util.Map<String, Promise> datasetUpdatePromises = new HashMap<>();
                Set<String> availableDatasetIds = new HashSet(result.result().keySet());

                for (String id :availableDatasetIds) {
                    graphManager.delete(id, r -> {
                        if(r.failed()){
                            LOGGER.info("Deletion of dataset graph failed.");
                        }
                    });
                }
                Set<String> messageDatasetIds = connector.getCatalog().getOffer().stream().map(r -> r.getId().toString()).collect(Collectors.toSet());
                Map<String, Resource> id2ResourceMap = new HashMap<>();

                for(String id : messageDatasetIds){
                    Resource dataset = connector.getCatalog().getOffer().stream().filter(r -> r.getId().toString().equals(id)).collect(Collectors.toList()).get(0);
                    id2ResourceMap.put(id, dataset);

                    try {
                        graphManager.create(dataset.getId().toString(),serializer.serialize(dataset),r->{
                            if(r.failed()){
                                LOGGER.info("Deletion of dataset graph failed.");
                            }
                        });
                    } catch (IOException e) {
                        LOGGER.error(e);
                        readyHandler.handle(Future.failedFuture(e));
                    }
                    availableDatasetIds.add(id);
                }
                for (String datasetId : availableDatasetIds) {
                    datasetUpdatePromises.put(datasetId, Promise.promise());
                }

                Collection<String> piveauIds = result.result().keySet();
                if (piveauIds.isEmpty() && messageDatasetIds.isEmpty()) {
                    idsService.handleSucceededMessage(uri, readyHandler);
                } else {
                    for (String messageId : messageDatasetIds) {
                        Resource dataset = id2ResourceMap.get(messageId);
                        try {
                            dcatTransformerService.transformDataset(serializer.serialize(dataset), ((StaticEndpoint)dataset.getResourceEndpoint().get(0)).getEndpointArtifact().getCreationDate().toString(), datasetTransformResult -> {
                                if(datasetTransformResult.succeeded()) {
                                    if (piveauIds.contains(messageId)) {
                                        updateDataset(datasetTransformResult.result(), messageId, catalogueId, datasetUpdatePromises);
                                    } else {
                                        String internalId = UUID.randomUUID().toString();
                                        createDataSet(datasetTransformResult.result(), messageId, internalId, catalogueId, datasetUpdatePromises);
                                    }
                                    piveauIds.remove(messageId);
                                } else {
                                    LOGGER.error(datasetTransformResult.cause());
                                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                                }
                            });
                        } catch (IOException e) {
                            LOGGER.error(e);
                            readyHandler.handle(Future.failedFuture(e));
                        }
                    }
                    CompositeFuture.all(messageDatasetIds.stream().map(datasetUpdatePromises::get).map(Promise::future).collect(Collectors.toList())).setHandler(mesFutures -> {
                        if(mesFutures.succeeded()) {
                            for (String orphan : piveauIds) {
                                deleteDatasetPiveau(result.result().get(orphan), catalogueId, res -> datasetManager.deleteByExternalId(orphan, deleteResult -> handleDataSetPromise(deleteResult, orphan, datasetUpdatePromises)));
                            }
                        } else {
                            LOGGER.error(mesFutures.cause());
                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                    CompositeFuture.all(datasetUpdatePromises.values().stream().map(Promise::future).collect(Collectors.toList())).setHandler(ac -> {
                        if (ac.succeeded()) {
                            idsService.handleSucceededMessage(uri, readyHandler);
                        } else {
                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                        }
                    });
                }
            } else {
                idsService.handleRejectionMessage(RejectionReason.NOT_FOUND, uri, readyHandler);
            }
        }));

    }

    private void createDataSet(String transformedDataset, String datasetExternalId, String dataSetId, String catalogueId, java.util.Map<String, Promise> datasetUpdatePromises) {
        piveauMessageService.createDataSet(transformedDataset, dataSetId, catalogueId, datasetReply -> {
            if (datasetReply.succeeded()) {
                datasetManager.create(datasetExternalId, dataSetId, datasetPersistenceReply2 -> {
                    if (datasetPersistenceReply2.succeeded()) {
                        datasetUpdatePromises.get(datasetExternalId).complete();
                    } else {
                        datasetUpdatePromises.get(datasetExternalId).fail(datasetPersistenceReply2.cause());
                    }
                });
            } else {
                LOGGER.error(datasetReply.cause());
            }
        });
    }

    private void updateDataset(String datasetTTL, String messageId, String catalogueId, java.util.Map<String, Promise> datasetUpdatePromises) {
        resolveDatasetIdForUpdate(messageId, reply -> {
            piveauMessageService.createDataSet(datasetTTL, reply.result(), catalogueId, datasetReply -> {
                if (datasetReply.succeeded()) {
                    datasetUpdatePromises.get(messageId).complete();
                } else {
                    LOGGER.error(datasetReply.cause());
                    datasetUpdatePromises.get(messageId).fail(datasetReply.cause());
                }
            });
        });
    }
    private void deleteDatasetPiveau(String datasetId, String catalogueId, Handler<AsyncResult<Void>> next) {
        piveauMessageService.deleteDataSet(datasetId, catalogueId, deleteAsset -> {
            if (deleteAsset.succeeded()) {
                next.handle(Future.succeededFuture());
            } else {
                LOGGER.error(deleteAsset.cause());
                next.handle(Future.failedFuture(deleteAsset.cause()));
            }
        });
    }

    private void handleDataSetPromise(AsyncResult<Void> reply, String idsId, java.util.Map<String, Promise> datasetdeletePromises) {
        if (reply.succeeded()) {
            datasetdeletePromises.get(idsId).complete();
            LOGGER.info("DataAsset From Database successfully deleted");
        } else {
            LOGGER.error(reply.cause());
            datasetdeletePromises.get(idsId).fail(reply.cause());
        }
    }

    private void resolvePiveauIds(AsyncResult<java.util.List<String>> piveauDatasetIds, Handler<AsyncResult<java.util.Map<String, String>>> completer) {
        java.util.Map<String, Promise<JsonObject>> piveau2IDSResolvePromiseMap = new HashMap<>();
        if (piveauDatasetIds.succeeded()) {
            for (String piveauId : piveauDatasetIds.result()) {
                Promise<JsonObject> idsResolve = Promise.promise();
                piveau2IDSResolvePromiseMap.put(piveauId, idsResolve);
                datasetManager.findByInternalId(piveauId, idsResolve);
            }
            CompositeFuture.all(piveau2IDSResolvePromiseMap.values().stream().map(Promise::future).collect(Collectors.toList())).setHandler(ac -> {
                if (ac.succeeded()) {
                    java.util.Map<String, String> resultMap = new HashMap<>();
                    for (String piveauId : piveau2IDSResolvePromiseMap.keySet()) {
                        resultMap.put(piveau2IDSResolvePromiseMap.get(piveauId).future().result().getString("external_id"), piveauId);
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

    private void resolveDatasetIdForUpdate(String dataassetIdExternal, Handler<AsyncResult<String>> next) {
        datasetManager.findByExternalId(dataassetIdExternal, datasetPersistenceReply -> {
            if (datasetPersistenceReply.succeeded()) {
                if (!datasetPersistenceReply.result().isEmpty()) {
                    String id = datasetPersistenceReply.result().getString("internal_id");
                    next.handle(Future.succeededFuture(id));
                } else {
                    String datId = UUID.randomUUID().toString();
                    next.handle(Future.succeededFuture(datId));
                }
            } else {
                LOGGER.error(datasetPersistenceReply.cause());
                next.handle(Future.failedFuture(datasetPersistenceReply.cause()));
            }
        });
    }
}
