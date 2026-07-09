package com.training.serializer;

import com.training.event.EmployeeDeletedEvent;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONEmployeeDeletedEventSerializer implements Serializer<EmployeeDeletedEvent>{
    @Override
    public byte[] serialize(EmployeeDeletedEvent event) {
        JSONObject json = new JSONObject();
        json.put("id", event.getEmployeeId().getValue());
        json.put("companyId", event.getCompanyId().getValue());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
