package com.itsaur.dto;

public class CompanyUpdateDTO {

    private String companyName;
    private String vat;
    private String address;

    public CompanyUpdateDTO() {
    }

    public CompanyUpdateDTO(String companyName, String vat, String address) {
        this.companyName = companyName;
        this.vat = vat;
        this.address = address;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getVat() {
        return vat;
    }

    public void setVat(String vat) {
        this.vat = vat;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
