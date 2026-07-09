package com.training.inMemory.search;

public class InMemoryEndsWithStrategy implements InMemoryConditionStrategy {
    @Override
    public boolean checkValue(Object fieldValue, Object searchValue) {
        return fieldValue.toString().toLowerCase()
                .endsWith(searchValue.toString().toLowerCase());
    }
}
