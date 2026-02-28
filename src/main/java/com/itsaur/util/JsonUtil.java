package com.itsaur.util;


import com.itsaur.model.Company;
import com.itsaur.model.Employee;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class JsonUtil {

    private JsonUtil(){}

    public static JSONObject toJson(Employee employee){
        return new JSONObject()
                .put("id", employee.getId())
                .put("firstname", employee.getFirstname())
                .put("lastname", employee.getLastname())
                .put("email", employee.getEmail())
                .put("companyId", employee.getCompanyId())
                .put("created_at", employee.getCreatedAt().toLocalDateTime())
                .put("updated_at", employee.getUpdatedAt().toLocalDateTime());
    }

//    public static JSONArray toJsonArray(List<Employee> employees){
//        JSONArray jsonArray = new JSONArray();
//        for (Employee e: employees){
//            jsonArray.put(toJson(e));
//        }
//        return jsonArray;
//    }

    public static JSONObject toJson(Company company){
        return new JSONObject()
                .put("id", company.getId())
                .put("company_name", company.getCompanyName())
                .put("vat", company.getVat())
                .put("address", company.getAddress())
                .put("created_at", company.getCreatedAt().toLocalDateTime())
                .put("updated_at", company.getUpdatedAt().toLocalDateTime());
    }

//    public static JSONArray toJsonArrayCompany(List<Company> companies){
//        JSONArray jsonArray = new JSONArray();
//        for (Company c: companies){
//            jsonArray.put(toJson(c));
//        }
//        return jsonArray;
//    }

    public static <T> JSONArray toJsonArray(List<T> items){
        JSONArray jsonArray = new JSONArray();
        for(T item : items){
            if (item instanceof Employee) {
                jsonArray.put(toJson((Employee) item));
            } else if (item instanceof Company) {
                jsonArray.put(toJson((Company) item));
            }
        }
        return jsonArray;
        }

}
