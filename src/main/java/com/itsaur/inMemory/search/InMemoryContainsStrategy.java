package com.itsaur.inMemory.search;

public class InMemoryContainsStrategy implements InMemoryConditionStrategy {
    @Override
    public boolean checkValue(Object fieldValue, Object searchValue) {
        return fieldValue.toString().toLowerCase()
                .contains(searchValue.toString().toLowerCase());
    }
}
