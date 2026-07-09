package com.training.serializer;

import com.training.model.Company;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONCompanySerializer implements Serializer<Company> {
    @Override
    public byte[] serialize(Company company) {
        JSONObject json =  new JSONObject();
        json.put("id", company.getId());
        json.put("companyName", company.getCompanyName());
        json.put("vat", company.getVat());
        json.put("address", company.getAddress());
        json.put("createdAt", company.getCreatedAt());
        json.put("updatedAt", company.getUpdatedAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
