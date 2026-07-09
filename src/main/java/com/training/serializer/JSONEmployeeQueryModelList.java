package com.training.serializer;

import com.training.model.EmployeeQueryModel;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JSONEmployeeQueryModelList implements Serializer<List<EmployeeQueryModel>> {
    @Override
    public byte[] serialize(List<EmployeeQueryModel> object) {
        JSONArray jsonArray = new JSONArray();
        for (EmployeeQueryModel employee : object) {
            JSONObject json = new JSONObject();
            json.put("id", employee.getId());
            json.put("firstname", employee.getFirstname());
            json.put("lastname", employee.getLastname());
            json.put("email", employee.getEmail());
            json.put("companyId", employee.getCompanyId());
            json.put("company_name", employee.getCompanyName());
            json.put("company_address", employee.getCompanyAddress());
            json.put("created_at", employee.getCreatedAt());
            json.put("updated_at", employee.getUpdatedAt());
            jsonArray.put(json);
        }
        return jsonArray.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
