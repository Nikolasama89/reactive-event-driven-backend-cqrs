package com.training.serializer;

import com.training.event.EmployeeDeletedEvent;
import com.training.model.CompanyId;
import com.training.model.EmployeeId;
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
