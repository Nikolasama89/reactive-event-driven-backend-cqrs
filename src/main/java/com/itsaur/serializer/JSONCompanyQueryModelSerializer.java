package com.itsaur.serializer;

import com.itsaur.model.CompanyQueryModel;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONCompanyQueryModelSerializer implements Serializer<CompanyQueryModel> {
    @Override
    public byte[] serialize(CompanyQueryModel object) {
        JSONObject json = new JSONObject();
        json.put("id", object.getId());
        json.put("company_name", object.getCompanyName());
        json.put("vat", object.getVat());
        json.put("address", object.getAddress());
        json.put("total_employees", object.getTotalEmployees());
        json.put("createdAt", object.getCreatedAt());
        json.put("updatedAt", object.getUpdatedAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
