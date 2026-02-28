package com.itsaur.serializer;

import com.itsaur.dto.EmployeeCreateDTO;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

public class JSONEmployeeCreateDeserializer implements Deserializer<EmployeeCreateDTO> {
    @Override
    public EmployeeCreateDTO deserialize(byte[] bytes) {
        JSONObject jsonObject = new JSONObject(new String(bytes));
        EmployeeCreateDTO employeeCreateDTO = new EmployeeCreateDTO();
        employeeCreateDTO.setFirstname(jsonObject.getString("firstname"));
        employeeCreateDTO.setLastname(jsonObject.getString("lastname"));
        employeeCreateDTO.setEmail(jsonObject.getString("email"));
        employeeCreateDTO.setCompanyId(jsonObject.getLong("companyId"));
        return employeeCreateDTO;
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Deserializer.super.contentType(response);
    }

}
