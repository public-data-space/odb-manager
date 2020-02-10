package de.fraunhofer.fokus.ids.services.dcatTransformerService;

import de.fraunhofer.iais.eis.*;
import de.fraunhofer.iais.eis.util.PlainLiteral;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;

import java.net.URI;
import java.util.ArrayList;

public class DCATTransformerServiceImpl implements DCATTransformerService {

    public DCATTransformerServiceImpl(Handler<AsyncResult<DCATTransformerService>> readyHandler){
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public DCATTransformerService transformCatalogue(String connectorJson, Handler<AsyncResult<String>> readyHandler) {
        Connector connector = Json.decodeValue(connectorJson, Connector.class);

        Model model = setPrefixes(ModelFactory.createDefaultModel());

        org.apache.jena.rdf.model.Resource resource = model.createResource(connector.getId().toString())
                .addProperty(RDF.type, DCAT.catalog)
                .addLiteral(DCTerms.type, "dcat-ap")
                //     .addProperty(DCTerms.language, "")
                //    .addProperty(DCTerms.spatial, connector.getPhysicalLocation().toString())
                .addLiteral(DCTerms.publisher, connector.getMaintainer().toString());
        addPLainLiterals(resource, connector.getTitle(), DCTerms.title);
        addPLainLiterals(resource, connector.getDescription(), DCTerms.description);

        model.write(System.out, "TTL");

        return this;
    }

    private void checkNull(Object object, Property property,org.apache.jena.rdf.model.Resource resource ){
        if (object!=null){
                resource.addProperty(property, String.valueOf(object));
        }
    }

    @Override
    public DCATTransformerService transformDataset(String datasetJson, Handler<AsyncResult<String>> readyHandler) {
        Resource dataasset = Json.decodeValue(datasetJson, Resource.class);

        Model model = setPrefixes(ModelFactory.createDefaultModel());

        org.apache.jena.rdf.model.Resource resource = model.createResource(dataasset.getId().toString())
                .addProperty(RDF.type, DCAT.Dataset);

        if (dataasset.getPublisher() != null ){
            checkNull(dataasset.getPublisher().getId(),DCTerms.publisher,resource);
        }

        checkNull(dataasset.getStandardLicense(),DCTerms.license,resource);
        checkNull(dataasset.getVersion(),DCTerms.hasVersion,resource);
        addPLainLiterals(resource, dataasset.getKeyword(),DCAT.keyword);

        if (dataasset.getTheme()!=null){
            for (URI uri:dataasset.getTheme()){
                checkNull(uri,DCAT.theme,resource);
            }
        }

        for (Endpoint endpoint:dataasset.getResourceEndpoint()){
            String string = endpoint.getEndpointHost().getId()+endpoint.getPath();
            resource.addProperty(DCAT.endpointURL,string);
        }

        if(dataasset.getLanguage() != null) {
            for (Language language : dataasset.getLanguage()) {
                resource.addLiteral(DCTerms.language, language.toString());
            }
        }
        addPLainLiterals(resource, dataasset.getTitle(), DCTerms.title);
        addPLainLiterals(resource, dataasset.getDescription(), DCTerms.description);
        model.write(System.out, "TTL");

        return this;
    }

    private void addPLainLiterals(org.apache.jena.rdf.model.Resource resource, ArrayList<? extends PlainLiteral> list, Property relation){
        if(list != null) {
            for (PlainLiteral literal : list) {
                resource.addLiteral(relation, literal.getValue());
            }
        }
    }

    private Model setPrefixes(Model model) {
        return model.setNsPrefix("dcat", DCAT.NS)
                .setNsPrefix("dct", DCTerms.NS)
                .setNsPrefix("foaf", FOAF.NS)
                .setNsPrefix("locn","<http://www.w3.org/ns/locn#>")
                .setNsPrefix("owl", OWL.NS)
                .setNsPrefix("rdf", RDF.uri)
                .setNsPrefix("rdfs", RDFS.uri)
                .setNsPrefix("schema","<http://schema.org/>")
                .setNsPrefix("skos", SKOS.uri)
                .setNsPrefix("time"," <http://www.w3.org/2006/time>")
                .setNsPrefix("vcard", VCARD.uri)
                .setNsPrefix("xml","<http://www.w3.org/XML/1998/namespace>")
                .setNsPrefix("xsd", XSD.NS);
    }

    public static void main(String[] args) {
        Connector c = Json.decodeValue(
                "{\n" +
                        "\"@type\" : \"ids:BaseConnector\",\n" +
                        "\"version\" : \"0.0.1\",\n" +
                        "\"securityProfile\" : {\n" +
                        "\"@id\" : \"https://w3id.org/idsa/code/BASE_CONNECTOR_SECURITY_PROFILE\"\n" +
                        "},\n" +
                        "\"catalog\" : {\n" +
                        "\"@type\" : \"ids:Catalog\",\n" +
                        "\"request\" : [ ],\n" +
                        "\"offer\" : [ ],\n" +
                        "\"@id\" : \"http://fokus.fraunhofer.de/odc#Catalog\"\n" +
                        "},\n" +
                        "\"maintainer\" : \"http://fokus.fraunhofer.de/\",\n" +
                        "\"curator\" : \"http://fokus.fraunhofer.de/\",\n" +
                        "\"inboundModelVersion\" : [ \"2.0.0\" ],\n" +
                        "\"title\" : [ {\n" +
                        "\"@value\" : \"Open Data Connector\"\n" +
                        "} ],\n" +
                        "\"outboundModelVersion\" : \"2.0.0\",\n" +
                        "\"@id\" : \"http://fokus.fraunhofer.de/odc#Connector\",\n" +
                        "\"host\" : [ {\n" +
                        "\"@type\" : \"ids:Host\",\n" +
                        "\"protocol\" : {\n" +
                        "\"@id\" : \"https://w3id.org/idsa/code/HTTP\"\n" +
                        "},\n" +
                        "\"accessUrl\" : \"http://fokus.fraunhofer.de/odc\",\n" +
                        "\"@id\" : \"https://w3id.org/idsa/autogen/host/144354dc-4747-438b-b194-721710d924a4\"\n" +
                        "} ]\n" +
                        "}", Connector.class);
        Future<DCATTransformerService> future = Future.future();
        Future<String> jFuture = Future.future();
        DCATTransformerServiceImpl t = new DCATTransformerServiceImpl(future.completer());
        t.transformCatalogue(Json.encode(c), jFuture.completer());
    }
}
