package com.training.serializer;

import com.training.dto.CreateResponseDTO;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;

public class JSONCreateResponseSerializer implements Serializer<CreateResponseDTO> {
    @Override
    public byte[] serialize(CreateResponseDTO object) {
        JSONObject json = new JSONObject();
        json.put("id", object.getId());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
}
