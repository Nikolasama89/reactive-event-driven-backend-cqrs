package com.itsaur.serializer;

import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.model.Address;
import com.itsaur.model.CompanyId;
import com.itsaur.model.CompanyName;
import com.itsaur.model.Vat;
import org.json.JSONObject;

import java.time.OffsetDateTime;

public class JSONCompanyUpdatedEventDeserializer implements Deserializer<CompanyUpdatedEvent>{
    @Override
    public CompanyUpdatedEvent deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new CompanyUpdatedEvent(
                CompanyId.create(jsonObject.getLong("id")),
                CompanyName.create(jsonObject.getString("company_name")),
                Vat.create(jsonObject.getString("vat")),
                Address.create(jsonObject.getString("address")),
                OffsetDateTime.parse(jsonObject.getString("updatedAt")));
    }
}
