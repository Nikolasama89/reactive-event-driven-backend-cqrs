package com.itsaur.serializer;

import com.itsaur.event.EmployeeUpdatedEvent;
import com.itsaur.model.*;
import org.json.JSONObject;

import java.time.OffsetDateTime;

public class JSONEmployeeUpdatedEventDeserializer implements Deserializer<EmployeeUpdatedEvent>{
    @Override
    public EmployeeUpdatedEvent deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new EmployeeUpdatedEvent(
                EmployeeId.create(jsonObject.getLong("id")),
                CompanyId.create(jsonObject.getLong("companyId")),
                FullName.create(jsonObject.getString("firstname"), jsonObject.getString("lastname")),
                Email.create(jsonObject.getString("email")),
                CompanyName.create(jsonObject.getString("companyName")),
                Address.create(jsonObject.getString("companyAddress")),
                OffsetDateTime.parse(jsonObject.getString("occurredAt")));
    }
}