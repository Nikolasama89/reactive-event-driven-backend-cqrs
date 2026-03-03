package com.itsaur.repository.search;

import com.itsaur.dto.enums.TypeCondition;

import java.util.Map;

public class DBConditionStrategyFactory {

    private static final Map<TypeCondition, DBConditionStrategy> strategies = Map.of(
            TypeCondition.EQUALS, new DBEqualsStrategy(),
            TypeCondition.STARTSWITH, new DBStartsWithStrategy(),
            TypeCondition.ENDSWITH, new DBEndsWithStrategy(),
            TypeCondition.CONTAINS, new DBContainsStrategy()
    );

    public static DBConditionStrategy getStrategy(TypeCondition type) {
        return strategies.get(type);
    }
}
