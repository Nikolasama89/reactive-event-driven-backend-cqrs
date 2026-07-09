package com.training.serializer;

import com.training.event.CompanyUpdatedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONCompanyUpdatedEventSerializer implements Serializer<CompanyUpdatedEvent>{
    @Override
    public byte[] serialize(CompanyUpdatedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getCompanyId().getValue());
        json.put("company_name", event.getCompanyName().getValue());
        json.put("vat", event.getVat().getValue());
        json.put("address", event.getAddress().getValue());
        json.put("occurredAt", event.getOccurredAt());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
