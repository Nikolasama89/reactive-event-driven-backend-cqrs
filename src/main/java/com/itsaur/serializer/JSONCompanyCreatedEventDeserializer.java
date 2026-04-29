package com.itsaur.serializer;

import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.model.Address;
import com.itsaur.model.CompanyId;
import com.itsaur.model.CompanyName;
import com.itsaur.model.Vat;
import org.json.JSONObject;

import java.time.OffsetDateTime;

public class JSONCompanyCreatedEventDeserializer implements Deserializer<CompanyCreatedEvent>{
    @Override
    public CompanyCreatedEvent deserialize(byte[] bytes) {

        String raw = new String(bytes);
        System.out.println("EVENT-----------------> " + raw);

        JSONObject jsonObject = new JSONObject(new String(bytes));
        return new CompanyCreatedEvent(
                CompanyId.create(jsonObject.getLong("id")),
                CompanyName.create(jsonObject.getString("company_name")),
                Vat.create(jsonObject.getString("vat")),
                Address.create(jsonObject.getString("address")),
                OffsetDateTime.parse(jsonObject.getString("createdAt")));
    }
}
