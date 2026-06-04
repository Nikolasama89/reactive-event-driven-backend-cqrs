package com.itsaur.serializer;

import com.itsaur.model.Company;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class StreamCompanySerializer implements Serializer<Company> {
    @Override
    public byte[] serialize(String topic, Company company) {
        if (company == null){
            return null;
        }
        JSONObject json = new JSONObject();
        json.put("id", company.getId().getValue());
        json.put("companyName", company.getCompanyName().getValue());
        json.put("vat", company.getVat().getValue());
        json.put("address", company.getAddress().getValue());
        json.put("createdAt", company.getCreatedAt());
        json.put("updatedAt", company.getUpdatedAt());

        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
