package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.manager.CatalogueManager;
import de.fraunhofer.fokus.ids.manager.DatasetManager;
import de.fraunhofer.fokus.ids.manager.GraphManager;
import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.piveauMessageService.PiveauMessageService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class RegisterController {
    private Logger LOGGER = LoggerFactory.getLogger(RegisterController.class.getName());

    private GraphManager graphManager;
    private CatalogueManager catalogueManager;
    private DatasetManager datasetManager;
    private IDSService idsService;
    private PiveauMessageService piveauMessageService;
    private DCATTransformerService dcatTransformerService;

    public RegisterController(Vertx vertx, GraphManager graphManager){
        this.graphManager = graphManager;
        this.catalogueManager = new CatalogueManager(vertx);
        this.datasetManager = new DatasetManager(vertx);
        this.idsService = new IDSService(vertx);
        this.piveauMessageService = PiveauMessageService.createProxy(vertx, "piveauMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
    }


    public void register(URI uri, Connector connector, Handler<AsyncResult<String>> readyHandler) {
        String catalogueId = UUID.randomUUID().toString();
        catalogueManager.getCatalogueByExternalId(connector.getId().toString(),next->{
            if (next.succeeded()) {
                LOGGER.info("Connector is already registered in the internal database. Rejecting AvailableMessage.");
                idsService.handleRejectionMessage(RejectionReason.BAD_PARAMETERS, uri, readyHandler);
            }
            else {
                graphManager.create(connector.getId().toString(),Json.encode(connector), graphCreationResult->{
                    if (graphCreationResult.succeeded()){
                        dcatTransformerService.transformCatalogue(Json.encode(connector), null, catalogueTTLResult -> {
                                if(catalogueTTLResult.succeeded()){
                                piveauMessageService.createCatalogue(catalogueTTLResult.result(), catalogueId, piveauCatalogueReply ->
                                        createCatalogueInternal(piveauCatalogueReply, catalogueId,  connector.getId().toString(), internalCatalogueReply ->
                                                handleDatasetCreation(internalCatalogueReply, uri, connector, catalogueId, readyHandler)));
                                }
                        });
                    }
                    else {
                        LOGGER.error(graphCreationResult.cause());
                        idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                    }
                });
            }
        });

    }

    private void createCatalogueInternal(AsyncResult<Void> reply, String catalogueId, String connectorId, Handler<AsyncResult<Void>> next) {
        if (reply.succeeded()) {
            catalogueManager.create(connectorId, catalogueId, cataloguePersistenceReply -> {
                if (cataloguePersistenceReply.succeeded()) {
                    next.handle(Future.succeededFuture());
                } else {
                    LOGGER.error(cataloguePersistenceReply.cause());
                    next.handle(Future.failedFuture(cataloguePersistenceReply.cause()));
                }
            });
        } else {
            next.handle(Future.failedFuture(reply.cause()));
        }
    }

    private void handleDatasetCreation( AsyncResult<Void> internalCatalogueCreationReply, URI uri, Connector connector, String catalogueId, Handler<AsyncResult<String>> readyHandler) {
        if (internalCatalogueCreationReply.succeeded()) {
                    java.util.Map<String, Future> dataassetCreateFutures = new HashMap<>();
                    if (connector.getCatalog() != null) {
                        for (Resource resource : connector.getCatalog().getOffer()) {
                            StaticEndpoint staticEndpoint = (StaticEndpoint) resource.getResourceEndpoint().get(0);
                            String date = staticEndpoint.getEndpointArtifact().getCreationDate().toString();
                            graphManager.create(resource.getId().toString(), Json.encode(resource), graphResult -> {
                                if (graphResult.succeeded()) {
                                    dcatTransformerService.transformDataset(Json.encode(resource), date, dataSetTransformResult -> {
                                        if (dataSetTransformResult.succeeded()) {
                                            dataassetCreateFutures.put(resource.getId().toString(), Future.future());
                                            String datasetId = UUID.randomUUID().toString();
                                            createDataSet(dataSetTransformResult.result(), resource.getId().toString(), datasetId, catalogueId, dataassetCreateFutures);
                                        } else {
                                            LOGGER.error(dataSetTransformResult.cause());
                                            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                                        }
                                    });
                                } else {
                                    LOGGER.error(graphResult.cause());
                                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                                }
                            });
                        }
                        CompositeFuture.all(new ArrayList<>(dataassetCreateFutures.values())).setHandler(ac -> {
                            if (ac.succeeded()) {
                                idsService.handleSucceededMessage(uri, readyHandler);
                            } else {
                                LOGGER.error(ac.cause());
                                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
                            }
                        });
                    }
        } else {
            idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR, uri, readyHandler);
        }
    }

    public void createDataSet(String transformedDataset, String datasetExternalId, String dataSetId, String catalogueId, java.util.Map<String, Future> datasetUpdateFutures) {
        piveauMessageService.createDataSet(transformedDataset, dataSetId, catalogueId, datasetReply -> {
            if (datasetReply.succeeded()) {
                datasetManager.create(datasetExternalId, dataSetId, datasetPersistenceReply2 -> {
                    if (datasetPersistenceReply2.succeeded()) {
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
}
