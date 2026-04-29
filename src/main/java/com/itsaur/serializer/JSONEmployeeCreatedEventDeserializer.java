package com.itsaur.serializer;

import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.model.*;
import org.json.JSONObject;

import java.time.OffsetDateTime;

public class JSONEmployeeCreatedEventDeserializer implements Deserializer<EmployeeCreatedEvent>{
    @Override
    public EmployeeCreatedEvent deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new EmployeeCreatedEvent(
                EmployeeId.create(jsonObject.getLong("id")),
                CompanyId.create(jsonObject.getLong("companyId")),
                FullName.create(jsonObject.getString("firstname"), jsonObject.getString("lastname")),
                Email.create(jsonObject.getString("email")),
                CompanyName.create(jsonObject.getString("companyName")),
                Address.create(jsonObject.getString("companyAddress")),
                OffsetDateTime.parse(jsonObject.getString("createdAt")));
    }
}