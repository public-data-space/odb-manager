package de.fraunhofer.fokus.ids.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

import java.net.MalformedURLException;
import java.net.URL;

public class JsonLdContextResolver {

    WebClient webClient;
    private URL SUPPORTED_CONTEXT_URL;

    public JsonLdContextResolver(Vertx vertx){
        webClient =  WebClient.create(vertx);
        try {
            SUPPORTED_CONTEXT_URL =  new URL("https://jira.iais.fraunhofer.de/stash/projects/ICTSL/repos/ids-infomodel-commons/raw/jsonld-context/2.0.0/context.jsonld");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public void resolve(Handler<AsyncResult<JsonObject>> resultHandler){
        webClient.get(80, SUPPORTED_CONTEXT_URL.getHost(), SUPPORTED_CONTEXT_URL.getPath())
                .putHeader("Accept", "application/json")
                .send(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar.result().bodyAsJsonObject()));
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
        });
    }
}
