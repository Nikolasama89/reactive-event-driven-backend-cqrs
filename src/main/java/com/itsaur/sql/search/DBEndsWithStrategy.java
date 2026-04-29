package com.itsaur.sql.search;

public class DBEndsWithStrategy implements DBConditionStrategy {
    @Override
    public String buildSearch(String column, int index) {
        return column + " ILIKE $" + index;
    }

    @Override
    public String searchValue(String value) {
        return "%" + value;
    }
}
