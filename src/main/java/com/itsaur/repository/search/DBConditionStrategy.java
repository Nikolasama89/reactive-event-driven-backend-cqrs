package com.itsaur.repository.search;

public interface DBConditionStrategy {
    String buildSearch(String column, int index);
    Object searchValue(String value);
}
