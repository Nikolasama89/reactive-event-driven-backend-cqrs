package com.itsaur.serializer;

import com.itsaur.model.EmployeeQueryModel;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONEmployeeQueryModel implements Serializer<EmployeeQueryModel> {

    @Override
    public byte[] serialize(EmployeeQueryModel object) {
            JSONObject json = new JSONObject();
            json.put("id", object.getId());
            json.put("firstname", object.getFirstname());
            json.put("lastname", object.getLastname());
            json.put("email", object.getEmail());
            json.put("company_id", object.getCompanyId());
            json.put("company_name", object.getCompanyName());
            json.put("address", object.getCompanyAddress());
            json.put("createdAt", object.getCreatedAt());
            json.put("updatedAt", object.getUpdatedAt());
            return json.toString().getBytes(StandardCharsets.UTF_8);

    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
