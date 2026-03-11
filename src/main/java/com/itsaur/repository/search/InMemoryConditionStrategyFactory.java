package com.itsaur.repository.search;

import com.itsaur.dto.enums.TypeCondition;

import java.util.Map;

public class InMemoryConditionStrategyFactory {
    private static final Map<TypeCondition, InMemoryConditionStrategy> strategies = Map.of(
            TypeCondition.EQUALS, new InMemoryEqualsStrategy(),
            TypeCondition.STARTSWITH, new InMemoryStartsWithStrategy(),
            TypeCondition.ENDSWITH, new InMemoryEndsWithStrategy(),
            TypeCondition.CONTAINS, new InMemoryContainsStrategy()
    );

    public static InMemoryConditionStrategy getStrategy(TypeCondition type){
        return strategies.get(type);
    }
}
