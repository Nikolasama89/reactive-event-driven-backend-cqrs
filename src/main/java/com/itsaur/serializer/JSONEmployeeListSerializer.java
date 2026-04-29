package com.itsaur.serializer;

import com.itsaur.model.Employee;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class JSONEmployeeListSerializer implements Serializer<List<Employee>> {

    @Override
    public byte[] serialize(List<Employee> object) {
        JSONArray jsonArray = new JSONArray();
        for (Employee employee : object) {
            JSONObject json = new JSONObject();
            json.put("id", employee.getId());
            json.put("firstname", employee.getFullName().getFirstname());
            json.put("lastname", employee.getFullName().getLastname());
            json.put("email", employee.getEmail());
            json.put("companyId", employee.getCompanyId());
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
