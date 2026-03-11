package com.itsaur.repository.search;

import org.bouncycastle.util.Objects;

public class InMemoryEqualsStrategy implements InMemoryConditionStrategy{
    @Override
    public boolean checkValue(Object fieldValue, Object searchValue) {
        return Objects.areEqual(fieldValue, searchValue);
    }
}
