package com.training.dto;


public class EmployeeCreateDTO {

    private String firstname;
    private String lastname;
    private String email;
    Long companyId;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public EmployeeCreateDTO() {}

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

    public Long getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Long companyId) {
        this.companyId = companyId;
    }

    public EmployeeCreateDTO(String firstname, String lastname, String email, Long companyId) {
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
        this.companyId = companyId;
    }
}
