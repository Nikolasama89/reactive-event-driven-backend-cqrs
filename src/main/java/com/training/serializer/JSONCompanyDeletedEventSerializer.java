package com.training.serializer;

import com.training.event.CompanyDeletedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONCompanyDeletedEventSerializer implements Serializer<CompanyDeletedEvent>{
    @Override
    public byte[] serialize(CompanyDeletedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getCompanyId().getValue());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}