package com.training.serializer;

import com.training.model.Company;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JSONCompanyListSerializer implements Serializer<List<Company>> {
    @Override
    public byte[] serialize(List<Company> companies) {
        JSONArray jsonArray = new JSONArray();
        for (Company company : companies) {
            JSONObject json = new JSONObject();
            json.put("id", company.getId());
            json.put("companyName", company.getCompanyName());
            json.put("vat", company.getVat());
            json.put("address", company.getAddress());
            json.put("createdAt", company.getCreatedAt());
            json.put("updatedAt", company.getUpdatedAt());
            jsonArray.put(json);
        }
        return jsonArray.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
