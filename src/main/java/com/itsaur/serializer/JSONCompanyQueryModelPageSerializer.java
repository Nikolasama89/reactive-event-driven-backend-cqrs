package com.itsaur.serializer;

import com.itsaur.dto.PageDTO;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.service.CompanyQueryService;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONCompanyQueryModelPageSerializer implements Serializer<PageDTO<CompanyQueryModel>> {
    @Override
    public byte[] serialize(PageDTO<CompanyQueryModel> pageDTO) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("page", pageDTO.getPage());
        jsonObject.put("size", pageDTO.getSize());
        jsonObject.put("totalPages", pageDTO.getTotalPages());
        jsonObject.put("totalElements", pageDTO.getTotalElements());
        jsonObject.put("hasNext", pageDTO.isHasNext());

        JSONArray jsonArray = new JSONArray();
        if (pageDTO.getItems() != null) {
            for (CompanyQueryModel company : pageDTO.getItems()) {
                JSONObject empObject = new JSONObject();
                empObject.put("id", company.getId());
                empObject.put("company_name", company.getCompanyName());
                empObject.put("vat", company.getVat());
                empObject.put("address", company.getAddress());
                empObject.put("total_employees", company.getTotalEmployees());
                empObject.put("createdAt", company.getCreatedAt());
                empObject.put("updatedAt", company.getUpdatedAt());
                jsonArray.put(empObject);
            }
            jsonObject.put("items", jsonArray);

        }
        return jsonObject.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Serializer.super.contentType(response);
    }
}
