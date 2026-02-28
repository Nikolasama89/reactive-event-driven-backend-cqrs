package com.itsaur.model;

import org.json.JSONObject;

import java.time.OffsetDateTime;

public class Employee {
    private Long id;
    private String firstname;
    private String lastname;
    private String email;
    private Long companyId;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;

    public Employee() {}

    public Employee(Long id, String firstname, String lastname, String email, Long companyId,
                    OffsetDateTime createdAt, OffsetDateTime updatedAt) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.companyId = companyId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;

    }

    public Employee(String firstname, String lastname, String email, Long companyId) {
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.companyId = companyId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Long companyId) {
        this.companyId = companyId;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                ", email='" + email + '\'' +
                ", companyId=" + companyId +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }

    public Employee parseFromJsonCreate(String jsonObject) {
        JSONObject json = new JSONObject(jsonObject);
        String firstname = json.getString("firstname").trim().toLowerCase();
        firstname = capitalizeFirstLetter(firstname);
        String lastname = json.getString("lastname").trim().toLowerCase();
        lastname = capitalizeFirstLetter(lastname);
        String email = json.getString("email").trim().toLowerCase();
        long companyId = json.getLong("companyId");

        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.companyId = companyId;

        return this;
    }

    public Employee parseFromJsonUpdate(String jsonObject) {
        JSONObject json = new JSONObject(jsonObject);
        String firstname = json.getString("firstname").trim().toLowerCase();
        firstname = capitalizeFirstLetter(firstname);
        String lastname = json.getString("lastname").trim().toLowerCase();
        lastname = capitalizeFirstLetter(lastname);

        this.firstname = firstname;
        this.lastname = lastname;

        return this;
    }


    public String capitalizeFirstLetter(String word) {
        if (word == null || word.isEmpty()) {
            return word;
        }
        return word.substring(0, 1).toUpperCase() + word.substring(1);
    }
}
