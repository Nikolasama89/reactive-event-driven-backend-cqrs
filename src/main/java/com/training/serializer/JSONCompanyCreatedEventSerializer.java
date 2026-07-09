package com.training.serializer;

import com.training.event.CompanyCreatedEvent;
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
        json.put("occurredAt", event.getOccurredAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
