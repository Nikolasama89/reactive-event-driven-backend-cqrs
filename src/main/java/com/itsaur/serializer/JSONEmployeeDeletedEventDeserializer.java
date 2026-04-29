package com.itsaur.serializer;

import com.itsaur.event.EmployeeDeletedEvent;
import com.itsaur.model.CompanyId;
import com.itsaur.model.EmployeeId;
import org.json.JSONObject;

public class JSONEmployeeDeletedEventDeserializer implements Deserializer<EmployeeDeletedEvent>{
    @Override
    public EmployeeDeletedEvent deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new EmployeeDeletedEvent(
                EmployeeId.create(jsonObject.getLong("id")),
                CompanyId.create(jsonObject.getLong("companyId")));
    }
}
