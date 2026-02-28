package com.itsaur.repository.search;

public class DBStartsWithStrategy implements DBConditionStrategy {
    @Override
    public String buildSearch(String column, int index) {
        return column + " ILIKE $" + index;
    }

    @Override
    public String searchValue(String value) {
        return value + "%";
    }
}
