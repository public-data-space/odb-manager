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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;

public class DCATTransformerServiceImpl implements DCATTransformerService {

    public DCATTransformerServiceImpl(Handler<AsyncResult<DCATTransformerService>> readyHandler){
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public DCATTransformerService transformCatalogue(String connectorJson, Handler<AsyncResult<String>> readyHandler) {
        Connector connector = Json.decodeValue(connectorJson, Connector.class);

        Model model = setPrefixes(ModelFactory.createDefaultModel());

        org.apache.jena.rdf.model.Resource catalogue = model.createResource(connector.getId().toString())
                .addProperty(RDF.type, DCAT.Catalog)
                .addLiteral(DCTerms.type, "dcat-ap")
                .addProperty(DCTerms.language, model.createProperty("http://publications.europa.eu/resource/authority/language/ENG"));

        org.apache.jena.rdf.model.Resource publisher = model.createResource("http://ids.fokus.fraunhofer.de/publisher/"+UUID.randomUUID().toString());
        publisher.addProperty(RDF.type, FOAF.Agent)
                .addLiteral(FOAF.name, connector.getMaintainer().toString());

        catalogue.addProperty(DCTerms.publisher, publisher);

        addPLainLiterals(catalogue, connector.getTitle(), DCTerms.title);
        addPLainLiterals(catalogue, connector.getDescription(), DCTerms.description);

        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
            model.write(baos, "TTL");
            readyHandler.handle(Future.succeededFuture(baos.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return this;
    }

    @Override
    public DCATTransformerService transformDataset(String datasetJson, Handler<AsyncResult<String>> readyHandler) {
        Resource dataasset = Json.decodeValue(datasetJson, Resource.class);

        Model model = setPrefixes(ModelFactory.createDefaultModel());

        org.apache.jena.rdf.model.Resource dataset = model.createResource(dataasset.getId().toString())
                .addProperty(RDF.type, DCAT.Dataset);

        if (dataasset.getPublisher() != null ){
            checkNull(dataasset.getPublisher().getId(),DCTerms.publisher,dataset);
        }

        checkNull(dataasset.getStandardLicense(),DCTerms.license,dataset);
        checkNull(dataasset.getVersion(),DCTerms.hasVersion,dataset);
        addPLainLiterals(dataset, dataasset.getKeyword(),DCAT.keyword);

        if (dataasset.getTheme()!=null){
            for (URI uri:dataasset.getTheme()){
                checkNull(uri,DCAT.theme,dataset);
            }
        }

        for (Endpoint endpoint:dataasset.getResourceEndpoint()){
            String string = endpoint.getEndpointHost().getId()+endpoint.getPath();
            dataset.addProperty(DCAT.endpointURL,string);
        }

        if(dataasset.getLanguage() != null) {
            for (Language language : dataasset.getLanguage()) {
                dataset.addLiteral(DCTerms.language, language.toString());
            }
        }
        addPLainLiterals(dataset, dataasset.getTitle(), DCTerms.title);
        addPLainLiterals(dataset, dataasset.getDescription(), DCTerms.description);

        StaticEndpoint endpoint = (StaticEndpoint) dataasset.getResourceEndpoint().get(0);

        String accessUrl = endpoint.getEndpointHost().getId() + endpoint.getPath() + endpoint.getEndpointArtifact().getFileName();
        String id = "http://example.org/"+ UUID.randomUUID().toString();
        org.apache.jena.rdf.model.Resource distribution = model.createResource(id)
                    .addProperty(RDF.type, DCAT.Distribution)
                    .addProperty(DCAT.accessURL, accessUrl)
                    .addProperty(DCTerms.title,"Distribution-"+endpoint.getEndpointArtifact().getFileName());

        dataset.addProperty(DCAT.distribution, id);

        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
            model.write(baos);
            readyHandler.handle(Future.succeededFuture(baos.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
                .setNsPrefix("locn","http://www.w3.org/ns/locn#")
                .setNsPrefix("owl", OWL.NS)
                .setNsPrefix("rdf", RDF.uri)
                .setNsPrefix("rdfs", RDFS.uri)
                .setNsPrefix("schema","http://schema.org/")
                .setNsPrefix("skos", SKOS.uri)
                .setNsPrefix("time","http://www.w3.org/2006/time")
                .setNsPrefix("vcard", VCARD.uri)
                .setNsPrefix("xml","http://www.w3.org/XML/1998/namespace")
                .setNsPrefix("xsd", XSD.NS);
    }

    private void checkNull(Object object, Property property,org.apache.jena.rdf.model.Resource resource ){
        if (object!=null){
            resource.addProperty(property, String.valueOf(object));
        }
    }
}
