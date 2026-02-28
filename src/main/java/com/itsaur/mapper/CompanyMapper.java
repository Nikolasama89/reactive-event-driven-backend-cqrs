package com.itsaur.mapper;

import com.itsaur.dto.CompanyCreateDTO;
import com.itsaur.model.Company;

public class CompanyMapper {

    public Company toEntity(CompanyCreateDTO companyCreateDTO) {
        Company company = new Company();
        company.setCompanyName(companyCreateDTO.getCompanyName());
        company.setVat(companyCreateDTO.getVat());
        company.setAddress(companyCreateDTO.getAddress());
        return company;
    }
}
