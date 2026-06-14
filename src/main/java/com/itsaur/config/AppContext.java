package com.itsaur.config;

import com.itsaur.service.CompanyQueryService;
import com.itsaur.service.CompanyService;
import com.itsaur.service.EmployeeQueryService;
import com.itsaur.service.EmployeeService;

public class AppContext {
    private final EmployeeService employeeService;
    private final CompanyService companyService;
    private final EmployeeQueryService employeeQueryService;
    private final CompanyQueryService companyQueryService;

    public AppContext(
            EmployeeService employeeService,
            CompanyService companyService,
            EmployeeQueryService employeeQueryService,
            CompanyQueryService companyQueryService
    ) {
        this.employeeService = employeeService;
        this.companyService = companyService;
        this.employeeQueryService = employeeQueryService;
        this.companyQueryService = companyQueryService;
    }

    public EmployeeService getEmployeeService() {
        return employeeService;
    }

    public CompanyService getCompanyService() {
        return companyService;
    }

    public EmployeeQueryService getEmployeeQueryService() {
        return employeeQueryService;
    }

    public CompanyQueryService getCompanyQueryService() {
        return companyQueryService;
    }
}
