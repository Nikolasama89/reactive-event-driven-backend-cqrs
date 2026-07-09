package com.training.handler;

import com.training.dto.*;
import com.training.model.Company;
import com.training.model.CompanyQueryModel;
import com.training.model.Employee;
import com.training.model.EmployeeQueryModel;
import com.training.repository.search.SearchComponent;
import com.training.serializer.*;
import com.training.service.CompanyQueryService;
import com.training.service.CompanyService;
import com.training.service.EmployeeQueryService;
import com.training.service.EmployeeService;

import java.util.List;

public class HttpHandlersFactory {

    public static EmployeeHandler createEmployeeHandler(
            EmployeeService employeeService,
            EmployeeQueryService employeeQueryService
    ) {
        Serializer<Employee> employeeSerializer = new JSONEmployeeSerializer();
        Serializer<EmployeeQueryModel> employeeQueryModelSerializer = new JSONEmployeeQueryModel();
        Serializer<List<EmployeeQueryModel>> employeeQueryListModelSerializer = new JSONEmployeeQueryModelList();
        Serializer<PageDTO<EmployeeQueryModel>> employeePageSerializer = new JSONEmployeeQueryModelPageSerializer();
        Deserializer<EmployeeCreateDTO> employeeCreate = new JSONEmployeeCreateDeserializer();
        Deserializer<EmployeeUpdateDTO> employeeUpdate = new JSONEmployeeUpdateDeserializer();
        Serializer<CreateResponseDTO> createResponseDTOSerializer = new JSONCreateResponseSerializer();
        Deserializer<SearchComponent> searchRequestDTODeserializer = new JSONSearchComponentQueryDeserializer();

        return new EmployeeHandler(
                employeeService,
                employeeQueryService,
                employeeSerializer,
                employeeCreate,
                employeeUpdate,
                createResponseDTOSerializer,
                employeeQueryModelSerializer,
                employeeQueryListModelSerializer,
                employeePageSerializer,
                searchRequestDTODeserializer
        );
    }

    public static CompanyHandler createCompanyHandler(
            CompanyService companyService,
            CompanyQueryService companyQueryService
    ) {
        Serializer<Company> companySerializer = new JSONCompanySerializer();
        Serializer<CompanyQueryModel> companyQueryModelSerializer = new JSONCompanyQueryModelSerializer();
        Serializer<List<CompanyQueryModel>> companyQueryModelListSerializer= new JSONCompanyQueryModelListSerializer();
        Serializer<PageDTO<CompanyQueryModel>> companyQueryModelPageSerializer= new JSONCompanyQueryModelPageSerializer();
        Deserializer<CompanyCreateDTO> companyCreate = new JSONCompanyCreateDeserializer();
        Deserializer<CompanyUpdateDTO> companyUpdate = new JSONCompanyUpdateDeserializer();
        Serializer<CreateResponseDTO> createResponseDTOSerializer = new JSONCreateResponseSerializer();
        Deserializer<SearchComponent> searchRequestDTODeserializer = new JSONSearchComponentQueryDeserializer();

        return new CompanyHandler(
                companyService,
                companyQueryService,
                companySerializer,
                companyCreate,
                companyUpdate,
                createResponseDTOSerializer,
                companyQueryModelSerializer,
                companyQueryModelListSerializer,
                companyQueryModelPageSerializer,
                searchRequestDTODeserializer
        );
    }

}