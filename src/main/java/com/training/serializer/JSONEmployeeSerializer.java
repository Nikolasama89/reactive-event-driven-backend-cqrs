package com.training.serializer;

import com.training.model.Employee;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONEmployeeSerializer implements Serializer<Employee> {
    @Override
    public byte[] serialize(Employee employee) {
        JSONObject json = new JSONObject();
        json.put("id", employee.getId());
        json.put("firstname", employee.getFullName().getFirstname());
        json.put("lastname", employee.getFullName().getLastname());
        json.put("email", employee.getEmail());
        json.put("companyId", employee.getCompanyId());
        json.put("created_at", employee.getCreatedAt());
        json.put("updated_at", employee.getUpdatedAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
