package com.itsaur.serializer;

import com.itsaur.model.Employee;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class StreamEmployeeSerializer implements Serializer<Employee> {
    @Override
    public byte[] serialize(String topic, Employee employee) {
        if (employee == null){
            return null;
        }
        JSONObject json = new JSONObject();
        json.put("id", employee.getId().getValue());
        json.put("firstname", employee.getFullName().getFirstname());
        json.put("lastname", employee.getFullName().getLastname());
        json.put("email", employee.getEmail().getValue());
        json.put("companyId", employee.getCompanyId().getValue());
        json.put("createdAt", employee.getCreatedAt());
        json.put("updatedAt", employee.getUpdatedAt());

        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
