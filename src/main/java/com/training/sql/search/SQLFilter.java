package com.training.sql.search;

import java.util.List;

public class SQLFilter {
    private final String whereClauses;
    private final List<Object> params;

    public SQLFilter(String whereClauses, List<Object> params){
        this.whereClauses = whereClauses;
        this.params = params;
    }

    public String getWhereClauses() {
        return whereClauses;
    }

    public List<Object> getParams() {
        return params;
    }
}
