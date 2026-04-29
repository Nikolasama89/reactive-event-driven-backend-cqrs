package com.itsaur.model;

import com.itsaur.exceptions.FirstnameInvalidCharacterLengthException;
import com.itsaur.exceptions.FirstnameRequiredException;
import com.itsaur.exceptions.LastnameInvalidCharacterLengthException;
import com.itsaur.exceptions.LastnameRequiredException;

import java.util.Objects;

public class FullName {
    private final String firstname;
    private final String lastname;

    private FullName(String firstname, String lastname){
        this.firstname = firstname;
        this.lastname = lastname;
    }

    public static FullName create(String firstname, String lastname){

        if (firstname == null){
            return null;
        }
        if (firstname.isBlank()){
            throw new FirstnameRequiredException("Firstname is required");
        }
        if (firstname.length() < 3 || firstname.length() > 50){
            throw new FirstnameInvalidCharacterLengthException("Firstname must be between 3 and 50 characters");
        }
        if (lastname == null){
            return null;
        }
        if (lastname.isBlank()){
            throw new LastnameRequiredException("Lastname is required");
        }
        if (lastname.length() < 3 || lastname.length() > 50){
            throw new LastnameInvalidCharacterLengthException("Lastname is required");
        }
        return new FullName(firstname, lastname);
    }

    public static FullName construct(String firstname, String lastname){
        if (firstname == null){
            return null;
        }
        if (lastname == null){
            return null;
        }
        return new FullName(firstname, lastname);
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastname() {
        return lastname;
    }

    @Override
    public String toString() {
        return "FullName{" +
                "firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullName fullName = (FullName) o;
        return Objects.equals(firstname, fullName.firstname) && Objects.equals(lastname, fullName.lastname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstname, lastname);
    }
}