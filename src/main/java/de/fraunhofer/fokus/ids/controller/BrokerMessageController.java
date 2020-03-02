package de.fraunhofer.fokus.ids.controller;

import de.fraunhofer.fokus.ids.services.IDSService;
import de.fraunhofer.fokus.ids.services.brokerMessageService.BrokerMessageService;
import de.fraunhofer.fokus.ids.services.databaseService.DatabaseService;
import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.IDSMessageParser;
import de.fraunhofer.iais.eis.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.entity.mime.content.ContentBody;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;


public class BrokerMessageController {
    private Logger LOGGER = LoggerFactory.getLogger(BrokerMessageController.class.getName());
    private BrokerMessageService brokerMessageService;
    private DCATTransformerService dcatTransformerService;
    private DatabaseService databaseService;
    private IDSService idsService;

    public BrokerMessageController(Vertx vertx) {
        this.idsService = new IDSService(vertx);
        this.brokerMessageService = BrokerMessageService.createProxy(vertx, "brokerMessageService");
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
        this.databaseService = DatabaseService.createProxy(vertx, "databaseService");
    }

    public void getData(String input, Handler<AsyncResult<String>> readyHandler) {
        Message header = IDSMessageParser.getHeader(input);
        if (header == null) {
            try {
                idsService.handleRejectionMessage(RejectionReason.MALFORMED_MESSAGE, new URI(String.valueOf(RejectionReason.MALFORMED_MESSAGE)), readyHandler);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            URI uri = header.getId();
            Connector connector = IDSMessageParser.getBody(input);
            try {
                if (header instanceof ConnectorAvailableMessage) {
                    LOGGER.info("AvailableMessage received.");
                    idsService.register(uri, connector, readyHandler);
                } else if (header instanceof ConnectorUnavailableMessage) {
                    LOGGER.info("UnavailableMessage received.");
                    idsService.unregister(uri, connector, readyHandler);
                } else if (header instanceof ConnectorUpdateMessage) {
                    LOGGER.info("UpdateMessage received.");
                    idsService.update(uri, connector, readyHandler);
                } else if (header instanceof SelfDescriptionRequest) {
                    LOGGER.info("UpdateMessage received.");
                    idsService.selfDescriptionRequest(uri, readyHandler);
                } else {
                    LOGGER.error(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED);
                    idsService.handleRejectionMessage(RejectionReason.MESSAGE_TYPE_NOT_SUPPORTED, uri, readyHandler);
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.error("Something went wrong while parsing the IDS message.");
            }
        }

    }

    public void about(Handler<AsyncResult<String>> resultHandler) {
        JsonObject jsonObject = new JsonObject();
        Future<Broker> brokerFuture = Future.future();
        idsService.buildBroker(jsonObject, brokerFuture.completer());
        idsService.handleBrokerFuture(brokerFuture, contentBodyAsyncResult -> {
            ContentBody contentBody = contentBodyAsyncResult.result();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                contentBody.writeTo(out);
            } catch (IOException e) {
                resultHandler.handle(Future.failedFuture(e.getMessage()));
            }
            resultHandler.handle(Future.succeededFuture(out.toString()));
        });
    }


}
