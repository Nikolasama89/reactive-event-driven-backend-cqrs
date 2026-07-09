package com.training.serializer;

import com.training.event.EmployeeCreatedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONEmployeeCreatedEventSerializer implements Serializer<EmployeeCreatedEvent>{
    @Override
    public byte[] serialize(EmployeeCreatedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getEmployeeId().getValue());
        json.put("companyId", event.getCompanyId().getValue());
        json.put("firstname", event.getFullName().getFirstname());
        json.put("lastname", event.getFullName().getLastname());
        json.put("companyName", event.getCompanyName().getValue());
        json.put("companyAddress", event.getCompanyAddress().getValue());
        json.put("email", event.getEmail().getValue());
        json.put("occurredAt", event.getOccurredAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
