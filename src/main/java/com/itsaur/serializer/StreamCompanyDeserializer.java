package com.itsaur.serializer;

import com.itsaur.model.Company;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class StreamCompanyDeserializer implements Deserializer<Company> {
    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null){
            return null;
        }

        String jsonString = new String(data, StandardCharsets.UTF_8);
        JSONObject json = new JSONObject(jsonString);

        return Company.construct(
                json.getLong("id"),
                json.getString("companyName"),
                json.getString("vat"),
                json.getString("address"),
                OffsetDateTime.parse(json.getString("createdAt")),
                OffsetDateTime.parse(json.getString("updatedAt"))
        );
    }
}