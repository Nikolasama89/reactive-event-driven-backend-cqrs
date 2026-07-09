package com.training.serializer;

import com.training.dto.EmployeeUpdateDTO;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

public class JSONEmployeeUpdateDeserializer implements Deserializer<EmployeeUpdateDTO> {
    @Override
    public EmployeeUpdateDTO deserialize(byte[] bytes) {
            JSONObject jsonObject = new JSONObject(new String(bytes));
            EmployeeUpdateDTO employeeUpdateDTO = new EmployeeUpdateDTO();
            employeeUpdateDTO.setFirstname(jsonObject.getString("firstname"));
            employeeUpdateDTO.setLastname(jsonObject.getString("lastname"));
            return employeeUpdateDTO;
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response) {
        return Deserializer.super.contentType(response);
    }

}
