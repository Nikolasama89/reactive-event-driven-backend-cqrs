package com.itsaur.serializer;

import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.model.CompanyId;
import org.json.JSONObject;

public class JSONCompanyDeletedEventDeserializer implements Deserializer<CompanyDeletedEvent>{
    @Override
    public CompanyDeletedEvent deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new CompanyDeletedEvent(CompanyId.create(jsonObject.getLong("id")));
    }
}