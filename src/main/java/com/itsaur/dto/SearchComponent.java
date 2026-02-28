package com.itsaur.dto;

import java.util.List;

public interface SearchComponent {
    void sqlBuild(StringBuilder sqlString, List<String> params);
}
