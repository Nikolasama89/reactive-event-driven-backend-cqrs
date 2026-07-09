package com.training.serializer;

import com.training.dto.PageDTO;
import com.training.model.Company;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONPageCompanySerializer implements Serializer<PageDTO<Company>>{
    @Override
    public byte[] serialize(PageDTO<Company> pageDTO) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("page", pageDTO.getPage());
        jsonObject.put("size", pageDTO.getSize());
        jsonObject.put("totalPages", pageDTO.getTotalPages());
        jsonObject.put("totalElements", pageDTO.getTotalElements());
        jsonObject.put("hasNext", pageDTO.isHasNext());

        JSONArray jsonArray = new JSONArray();
        if(pageDTO.getTotalElements() != 0) {
            for (Company company: pageDTO.getItems()){
                JSONObject compObject = new JSONObject();
                compObject.put("id", company.getId());
                compObject.put("companyName", company.getCompanyName());
                compObject.put("vat", company.getVat());
                compObject.put("companyAddress", company.getAddress());
                compObject.put("createdAt", company.getCreatedAt());
                compObject.put("updatedAt", company.getUpdatedAt());
                jsonArray.put(compObject);
            }
            jsonObject.put("items", jsonArray);
        }
        return  jsonObject.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
