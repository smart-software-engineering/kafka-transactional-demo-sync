package com.sse.transactional.rest;

import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import java.util.List;

@Path("")
public class RestResource {
    @Inject
    private TransactionalListener transactionalListener;
    @Inject
    private DbSender dbSender;

    @GET
    @Path("/count")
    @Produces("application/json")
    public int count() {
        return transactionalListener.counter();
    }

    @GET
    @Path("/send")
    @Produces("application/json")
    public List<RecordInfo> send() {
        return dbSender.execute();
    }

    @GET
    @Path("/quote")
    @Produces("application/json")
    public JsonObject addQuote() {
        String fact = dbSender.insert();
        return Json.createObjectBuilder().add("fact", fact).build();
    }
}
