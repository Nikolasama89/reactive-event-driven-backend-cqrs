package com.training.serializer;

import com.training.model.Employee;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class StreamEmployeeDeserializer implements Deserializer<Employee> {
    @Override
    public Employee deserialize(String topic, byte[] data) {
        if (data == null){
            return null;
        }

        String jsonString = new String(data, StandardCharsets.UTF_8);
        JSONObject json = new JSONObject(jsonString);

        return Employee.construct(
                json.getLong("id"),
                json.getString("firstname"),
                json.getString("lastname"),
                json.getString("email"),
                json.getLong("companyId"),
                OffsetDateTime.parse(json.getString("createdAt")),
                OffsetDateTime.parse(json.getString("updatedAt"))
        );
    }
}