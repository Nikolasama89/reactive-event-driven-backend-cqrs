package com.itsaur.model;

import com.itsaur.exceptions.CompanyVatInvalidInputException;
import com.itsaur.exceptions.CompanyVatRequiredException;

import java.util.Objects;

public class Vat {

    private static final String VAT_REGEX = "^\\d{10}$";

    private final String value;

    private Vat(String value) {
        this.value = value;
    }

    public static Vat create(String value) {

        if (value == null){
            return null;
        }

        if (value.isBlank()) {
            throw new CompanyVatRequiredException("VAT is required.");
        }

        if (!value.matches(VAT_REGEX)) {
            throw new CompanyVatInvalidInputException("VAT should be 10 digits.");
        }

        return new Vat(value);
    }

    public static Vat construct(String value) {
        if (value == null) {
            return null;
        }
        return new Vat(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vat vat = (Vat) o;
        return Objects.equals(value, vat.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
