package com.training.model;

import com.training.exceptions.CompanyNameInvalidCharacterLengthException;
import com.training.exceptions.CompanyNameRequiredException;

import java.util.Objects;

public class CompanyName {
    private final String value;

    private CompanyName(String value) {
        this.value = value;
    }

    public static CompanyName create(String value){

        if (value == null){
            return null;
        }
        if (value.isEmpty()){
            throw new CompanyNameRequiredException("Company Name is required.");
        }
        if (value.length() < 3 || value.length() > 50){
            throw new CompanyNameInvalidCharacterLengthException("Company name length should be between 3 and 50 characters.");
        }
        return new CompanyName(value);
    }

    public static CompanyName construct(String value) {
        if (value == null) {
            return null;
        }
        return new CompanyName(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CompanyName{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompanyName that = (CompanyName) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
