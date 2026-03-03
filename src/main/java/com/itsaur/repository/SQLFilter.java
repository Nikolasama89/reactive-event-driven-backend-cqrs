package com.itsaur.repository;

import java.util.List;

public class SQLFilter {
    private final String whereClause;
    private final List<Object> params;

    public SQLFilter(String whereClause, List<Object> params) {
        this.whereClause = whereClause;
        this.params = params;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public List<Object> getParams() {
        return params;
    }
}
