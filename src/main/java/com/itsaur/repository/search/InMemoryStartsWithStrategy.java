package com.itsaur.repository.search;

public class InMemoryStartsWithStrategy implements InMemoryConditionStrategy{
    @Override
    public boolean checkValue(Object fieldValue, Object searchValue) {
        return fieldValue.toString().toLowerCase()
                .startsWith(searchValue.toString().toLowerCase());
    }
}
