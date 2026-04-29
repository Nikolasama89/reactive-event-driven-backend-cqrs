package com.itsaur.serializer;

import com.itsaur.event.EmployeeUpdatedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class JSONEmployeeUpdatedEventSerializer implements Serializer<EmployeeUpdatedEvent>{
    @Override
    public byte[] serialize(EmployeeUpdatedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getEmployeeId().getValue());
        json.put("companyId", event.getCompanyId().getValue());
        json.put("firstname", event.getFullName().getFirstname());
        json.put("lastname", event.getFullName().getLastname());
        json.put("email", event.getEmail().getValue());
        json.put("companyName", event.getCompanyName().getValue());
        json.put("companyAddress", event.getCompanyAddress().getValue());
        json.put("createdAt", event.getOccurredAt());
        json.put("updatedAt", OffsetDateTime.now());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
