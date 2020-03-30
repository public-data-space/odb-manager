package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import de.fraunhofer.iais.eis.RejectionReason;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.net.URI;

public class QueryMessageController {
    private TSConnector tsConnector;
    private IDSService idsService;
    private Logger LOGGER = LoggerFactory.getLogger(QueryMessageController.class.getName());


    public QueryMessageController(TSConnector tsConnector, Vertx vertx){
        this.tsConnector = tsConnector;
        this.idsService = new IDSService(vertx,tsConnector);
    }

    public void queryMessage(String query , URI correlationMessageURI, Handler<AsyncResult<String>> resultHandler) {
        tsConnector.query(query,"application/json",httpResponseAsyncResult -> {
            if (httpResponseAsyncResult.succeeded()) {
                LOGGER.info("Query Message succeeded");
                idsService.createResultMessage(correlationMessageURI, reply -> {
                if(reply.succeeded()){
                    idsService.createMultiPartMessage(correlationMessageURI, reply.result(), httpResponseAsyncResult.result().bodyAsJsonObject(),resultHandler);
                } else {
                    LOGGER.error(reply.cause());
                    idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR,correlationMessageURI,resultHandler);
                }
                });
            }
            else{
                LOGGER.error(httpResponseAsyncResult.cause());
                idsService.handleRejectionMessage(RejectionReason.INTERNAL_RECIPIENT_ERROR,correlationMessageURI,resultHandler);
            }
        });
    }
}
