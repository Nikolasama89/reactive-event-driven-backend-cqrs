package com.itsaur.serializer;

import com.itsaur.event.CompanyCreatedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONCompanyCreatedEventSerializer implements Serializer<CompanyCreatedEvent>{
    @Override
    public byte[] serialize(CompanyCreatedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getCompanyId().getValue());
        json.put("company_name", event.getCompanyName().getValue());
        json.put("vat", event.getVat().getValue());
        json.put("address", event.getAddress().getValue());
        json.put("createdAt", event.getOccurredAt());
        json.put("updatedAt", event.getOccurredAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
