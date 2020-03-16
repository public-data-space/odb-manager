package de.fraunhofer.fokus.ids.manager;

import de.fraunhofer.fokus.ids.services.dcatTransformerService.DCATTransformerService;
import de.fraunhofer.fokus.ids.utils.TSConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.IOException;

public class GraphManager {
    private DCATTransformerService dcatTransformerService;
    private TSConnector tsConnector;

    public GraphManager(Vertx vertx, TSConnector tsConnector){
        this.dcatTransformerService = DCATTransformerService.createProxy(vertx, "dcatTransformerService");
        this.tsConnector = tsConnector;
    }

    public void create(String uri, String json, Handler<AsyncResult<HttpResponse<Buffer>>> resultHandler){
        dcatTransformerService.transformJsonForVirtuoso(json,stringAsyncResult -> {
            Model model = ModelFactory.createDefaultModel();
            try {
                model.read(IOUtils.toInputStream(stringAsyncResult.result(), "UTF-8"), null, "JSON-LD");
                tsConnector.putGraph(uri ,model, resultHandler);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    public void update(String uri, String json, Handler<AsyncResult<HttpResponse<Buffer>>> readyHandler){
        tsConnector.deleteGraph(uri,deleteAsync->{
            if (deleteAsync.succeeded()){
                create(uri,json,readyHandler);
            }
            else {
                readyHandler.handle(Future.failedFuture(deleteAsync.cause()));
            }
        });
    }

    public void delete(String uri, Handler<AsyncResult<HttpResponse<Buffer>>> readyHandler){
        tsConnector.deleteGraph(uri,readyHandler);
    }
}
