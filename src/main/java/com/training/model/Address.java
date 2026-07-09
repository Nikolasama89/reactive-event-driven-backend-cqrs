package com.training.model;

import com.training.exceptions.CompanyAddressCharacterLengthException;
import com.training.exceptions.CompanyAddressRequiredException;

import java.util.Objects;

public class Address {
    private final String value;

    private Address(String value) {
        this.value = value;
    }

    public static Address create(String value){

        if (value == null){
            return null;
        }

        if (value.isEmpty()){
            throw new CompanyAddressRequiredException("Company address is required.");
        }

        if (value.length() < 3 || value.length() > 50){
            throw new CompanyAddressCharacterLengthException("Address must be must be between 3 and 50 characters");
        }
        return new Address(value);
    }

    public static Address construct(String value){
        if (value == null){
            return null;
        }
        return new Address(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Address{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Address address = (Address) o;
        return Objects.equals(value, address.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
