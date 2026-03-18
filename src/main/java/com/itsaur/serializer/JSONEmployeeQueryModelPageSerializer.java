package com.itsaur.serializer;

import com.itsaur.dto.PageDTO;
import com.itsaur.model.EmployeeQueryModel;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.nio.charset.StandardCharsets;

public class JSONEmployeeQueryModelPageSerializer implements Serializer<PageDTO<EmployeeQueryModel>> {
    @Override
    public byte[] serialize(PageDTO<EmployeeQueryModel> pageDTO) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("page", pageDTO.getPage());
        jsonObject.put("size", pageDTO.getSize());
        jsonObject.put("totalPages", pageDTO.getTotalPages());
        jsonObject.put("totalElements", pageDTO.getTotalElements());
        jsonObject.put("hasNext", pageDTO.isHasNext());

        JSONArray jsonArray = new JSONArray();
        if (pageDTO.getItems() != null) {
            for (EmployeeQueryModel employee : pageDTO.getItems()) {
                JSONObject empObject = new JSONObject();
                empObject.put("id", employee.getId());
                empObject.put("firstname", employee.getFirstname());
                empObject.put("lastname", employee.getLastname());
                empObject.put("email", employee.getEmail());
                empObject.put("companyId", employee.getCompanyId());
                empObject.put("companyName", employee.getCompanyName());
                empObject.put("companyAddress", employee.getCompanyAddress());
                empObject.put("createdAt", employee.getCreatedAt());
                empObject.put("updatedAt", employee.getUpdatedAt());
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
