package com.training.model;

import com.training.exceptions.EmailInvalidCharacterLengthException;
import com.training.exceptions.EmailRequiredException;

import java.util.Objects;

public class Email {
    private final String value;

    private Email(String value){
        this.value = value;
    }

    public static Email create(String value){

        if (value == null){
            return null;
        }

        if (value.isEmpty()){
            throw new EmailRequiredException("Email is required.");
        }
        if (value.length() < 3 || value.length() > 50){
            throw new EmailInvalidCharacterLengthException("Firstname must be between 3 and 50 characters");
        }
        return new Email(value);
    }

    public static Email construct(String value){
        if (value == null){
            return null;
        }
        return new Email(value);
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Email{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Email email = (Email) o;
        return Objects.equals(value, email.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
