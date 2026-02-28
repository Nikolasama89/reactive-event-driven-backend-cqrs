package com.itsaur.serializer;

import com.itsaur.dto.CompanyUpdateDTO;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

public class JSONCompanyUpdateDeserializer implements Deserializer<CompanyUpdateDTO> {
    @Override
    public CompanyUpdateDTO deserialize(byte[] bytes) {
        JSONObject json = new JSONObject(new String(bytes));
        CompanyUpdateDTO companyUpdateDTO = new CompanyUpdateDTO();
        companyUpdateDTO.setCompanyName(json.getString("companyName"));
        companyUpdateDTO.setVat(json.getString("vat"));
        companyUpdateDTO.setAddress(json.getString("address"));
        return companyUpdateDTO;
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Deserializer.super.contentType(response);
    }

}
