package com.itsaur.serializer;

import com.itsaur.model.CompanyQueryModel;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JSONCompanyQueryModelListSerializer implements Serializer<List<CompanyQueryModel>> {
    @Override
    public byte[] serialize(List<CompanyQueryModel> object) {
        JSONArray jsonArray = new JSONArray();
        for (CompanyQueryModel company: object){
            JSONObject json = new JSONObject();
            json.put("id", company.getId());
            json.put("company_name", company.getCompanyName());
            json.put("vat", company.getVat());
            json.put("address", company.getAddress());
            json.put("total_employees", company.getTotalEmployees());
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
