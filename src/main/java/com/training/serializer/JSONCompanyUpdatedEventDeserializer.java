package com.training.serializer;

import com.training.event.CompanyUpdatedEvent;
import com.training.model.Address;
import com.training.model.CompanyId;
import com.training.model.CompanyName;
import com.training.model.Vat;
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
                OffsetDateTime.parse(jsonObject.getString("occurredAt")));
    }
}
