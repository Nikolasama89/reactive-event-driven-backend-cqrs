package com.training.serializer;

import com.training.dto.CompanyCreateDTO;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

public class JSONCompanyCreateDeserializer implements Deserializer<CompanyCreateDTO>{
    @Override
    public CompanyCreateDTO deserialize(byte[] bytes) {
        JSONObject json = new JSONObject(new String(bytes));
        CompanyCreateDTO companyCreateDTO = new CompanyCreateDTO();
        companyCreateDTO.setCompanyName(json.getString("companyName"));
        companyCreateDTO.setVat(json.getString("vat"));
        companyCreateDTO.setAddress(json.getString("address"));
        return companyCreateDTO;
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Deserializer.super.contentType(response);
    }

}
