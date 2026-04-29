package com.itsaur.serializer;

import com.itsaur.repository.search.SearchComponent;
import com.itsaur.repository.search.ComplexSearchRequestDTO;
import com.itsaur.repository.search.SimpleSearchRequestDTO;
import com.itsaur.dto.enums.SearchFieldCondition;
import com.itsaur.dto.enums.LogicOperator;
import com.itsaur.dto.enums.TypeCondition;
import com.itsaur.exceptions.InvalidSearchValueException;
import org.json.JSONArray;
import org.json.JSONObject;
import reactor.netty.http.server.HttpServerResponse;

import java.util.ArrayList;
import java.util.List;

public class JSONSearchComponentQueryDeserializer implements Deserializer<SearchComponent> {

    @Override
    public SearchComponent deserialize(byte[] bytes) {
        JSONObject object = new JSONObject(new String(bytes));
        return deserializeObject(object);
    }

    @Override
    public HttpServerResponse contentType(HttpServerResponse response){
        return Deserializer.super.contentType(response);
    }

    private SearchComponent deserializeObject(JSONObject object){

        if(object.has("conditions") && !object.getJSONArray("conditions").isEmpty()) {
            ComplexSearchRequestDTO complexSearchRequestDTO = new ComplexSearchRequestDTO();
            complexSearchRequestDTO.setType(LogicOperator.valueOf(object.getString("type").toUpperCase()));

            JSONArray conditions = object.getJSONArray("conditions");
            List<SearchComponent> searchComponents = new ArrayList<>();
            for(int i = 0; i < conditions.length(); i++) {
                JSONObject childObject = conditions.getJSONObject(i);

                SearchComponent child = deserializeObject(childObject);
                searchComponents.add(child);
            }
            complexSearchRequestDTO.setConditions(searchComponents);
            return complexSearchRequestDTO;
        }

        SimpleSearchRequestDTO simpleSearchRequestDTO = new SimpleSearchRequestDTO();
        simpleSearchRequestDTO.setType(TypeCondition.valueOf(object.getString("type").toUpperCase()));
        simpleSearchRequestDTO.setSearchField(SearchFieldCondition.valueOf(object.getString("field").toUpperCase()));

        Object rawObject = object.get("value");

        if (rawObject == null){
            throw new IllegalArgumentException("Search value can not be empty");
        }
        if (!(rawObject instanceof String value)){
            throw new InvalidSearchValueException("Search value must be letters");
        }

        simpleSearchRequestDTO.setValue(value);
        return simpleSearchRequestDTO;
    }
}