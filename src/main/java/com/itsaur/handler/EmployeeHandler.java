package com.itsaur.handler;

import com.itsaur.dto.*;
import com.itsaur.exceptions.*;
import com.itsaur.model.Employee;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.serializer.*;
import com.itsaur.service.EmployeeQueryService;
import com.itsaur.service.EmployeeService;
import io.netty.handler.codec.http.QueryStringDecoder;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.HttpServerRoutes;

import java.util.List;
import java.util.Objects;

public class EmployeeHandler {

    private final EmployeeService service;
    private final EmployeeQueryService queryService;
    private final Serializer<Employee> serializer;
    private final Deserializer<EmployeeCreateDTO> deserializerCreate;
    private final Deserializer<EmployeeUpdateDTO> deserializerUpdate;
    private final Serializer<CreateResponseDTO> responseSerializer;
    private final Serializer<EmployeeQueryModel> employeeQueryModelSerializer;
    private final Serializer<List<EmployeeQueryModel>> employeeListQueryModelSerializer;
    private final Serializer<PageDTO<EmployeeQueryModel>> employeePageQueryModelSerializer;
    private final Deserializer<SearchComponent> searchRequestDeserializer;


    public EmployeeHandler(
            EmployeeService service,
            EmployeeQueryService queryService,
            Serializer<Employee> serializer,
            Deserializer<EmployeeCreateDTO> deserializerCreate,
            Deserializer<EmployeeUpdateDTO> deserializerUpdate,
            Serializer<CreateResponseDTO> responseSerializer,
            Serializer<EmployeeQueryModel> employeeQueryModelSerializer,
            Serializer<List<EmployeeQueryModel>> employeeListQueryModelSerializer,
            Serializer<PageDTO<EmployeeQueryModel>> employeePageQueryModelSerializer,
            Deserializer<SearchComponent> searchRequestDeserializer
    ) {
        this.service = service;
        this.queryService = queryService;
        this.serializer = serializer;
        this.deserializerCreate = deserializerCreate;
        this.deserializerUpdate = deserializerUpdate;
        this.responseSerializer = responseSerializer;
        this.employeeQueryModelSerializer = employeeQueryModelSerializer;
        this.employeeListQueryModelSerializer = employeeListQueryModelSerializer;
        this.employeePageQueryModelSerializer = employeePageQueryModelSerializer;
        this.searchRequestDeserializer = searchRequestDeserializer;

    }

    public Mono<Void> getAllEmployees(HttpServerRequest req, HttpServerResponse res) {
        return queryService.findAllEmployees()
                .collectList()
                .flatMap(list -> {
                    byte[] bytes = employeeListQueryModelSerializer.serialize(list);
                    return serializer.contentType(res)
                            .sendByteArray(Mono.just(bytes))
                            .then();
                });
    }

    public Mono<Void> getEmployeeById(HttpServerRequest req, HttpServerResponse res) {

        String paramId = req.param("id");
        if (paramId == null || paramId.isBlank()) {
            return res.status(404).sendString(Mono.just("Id can not be empty")).then();
        }
        Long id = Long.parseLong(paramId);
        return queryService.findEmployeeById(id)
                .flatMap(employee -> {
            byte[] bytes = employeeQueryModelSerializer.serialize(employee);
            return employeeQueryModelSerializer.contentType(res)
                    .sendByteArray(Mono.just(bytes))
                    .then();
        }).onErrorResume(ex -> {
            if (ex instanceof EmployeeNotFoundException) {
                return res.status(404).sendString(Mono.just(ex.getMessage())).then();
            }
            if (ex instanceof EntityInvalidArgumentException) {
                return res.status(400).sendString(Mono.just(ex.getMessage())).then();
            }
            return res.status(500).sendString(Mono.just("Internal Server Error")).then();
        });
    }

    public Mono<Void> deleteEmployeeById(HttpServerRequest req, HttpServerResponse res) {
        String paramId = req.param("id");

        if (paramId == null || paramId.isBlank()) {
            return res.status(400).sendString(Mono.just("Id can not be empty")).then();
        }
        Long id = Long.parseLong(paramId);
        return service.delete(id)
                .then(res.status(204).send())
                .onErrorResume(ex -> {
                    if (ex instanceof EmployeeNotFoundException) {
                        return res.status(404)
                                .sendString(Mono.just("Employee with id: " + id + " not found")).then();
                    }
                    if (ex instanceof EntityInvalidArgumentException) {
                        return res.status(400).sendString(Mono.just(ex.getMessage())).then();
                    }
                    return res.status(500)
                            .sendString(Mono.just("Internal Server Error"))
                            .then();
                });
    }

    public Mono<Void> saveEmployee(HttpServerRequest req, HttpServerResponse res) {

        return req.receive()
                .aggregate()
                .asByteArray()
                .map(deserializerCreate::deserialize)
                .flatMap(service::save)
                .flatMap(saved -> {
                    res.status(201);
                    CreateResponseDTO responseDTO = new CreateResponseDTO(saved.getId().getValue());
                    deserializerCreate.contentType(res);
                    return res.sendByteArray(Mono.just(responseSerializer.serialize(responseDTO))).then();
                })
                .onErrorResume(error -> {
                            if (error instanceof EmailAlreadyExistsException) {
                                return res.status(409).sendString(Mono.just(error.getMessage())).then();
                            }

                            if ( error instanceof FirstnameRequiredException ||
                                    error instanceof FirstnameInvalidCharacterLengthException ||
                                    error instanceof LastnameRequiredException ||
                                    error instanceof LastnameInvalidCharacterLengthException ||
                                    error instanceof EmailInvalidCharacterLengthException ||
                                    error instanceof EmailRequiredException ||
                                    error instanceof CompanyIdInvalidException
                            ) {
                                return res.status(400).sendString(Mono.just(error.getMessage())).then();
                            }

                            if (error instanceof CompanyNotFoundException) {
                                return res.status(404).sendString(Mono.just(error.getMessage())).then();
                            }

                            return res.status(500).sendString(Mono.just(error.getMessage())).then();
                        });
    }

    public Mono<Void> updateEmployee(HttpServerRequest req, HttpServerResponse res) {

        return Mono.fromCallable(() -> Long.parseLong(Objects.requireNonNull(req.param("id"))))
                .flatMap(id -> req.receive()
                        .aggregate()
                        .asByteArray()
                        .map(deserializerUpdate::deserialize)
                        .map( employee -> new EmployeeUpdateDTO(employee.getFirstname(), employee.getLastname()))
                        .flatMap(updated -> service.update(id, updated))
                        .then(res.status(204).send()))
                .onErrorResume(error -> {
                    if (error instanceof FirstnameRequiredException ||
                            error instanceof LastnameRequiredException ||
                            error instanceof FirstnameInvalidCharacterLengthException ||
                            error instanceof LastnameInvalidCharacterLengthException
                    ) {
                        return res.status(400).sendString(Mono.just(error.getMessage())).then();
                    }

                    if (error instanceof EmployeeNotFoundException){
                        return res.status(404).sendString(Mono.just(error.getMessage())).then();
                    }

                    return res.status(500).sendString(Mono.just("Internal Server Error")).then();
                });
    }

    public Mono<Void> getPage(HttpServerRequest req, HttpServerResponse res) {

        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(req.uri());

        // Αν δεν δωσει ο χρηστης
        String pageParam = queryStringDecoder.parameters().getOrDefault("page", List.of("0")).get(0);
        if(pageParam.isBlank()) {
            pageParam = "0";
        }

        String sizeParam = queryStringDecoder.parameters().getOrDefault("size", List.of("10")).get(0);
        if(sizeParam.isBlank()) {
            sizeParam = "10";
        }

        String nameParam = queryStringDecoder.parameters().getOrDefault("name", List.of("")).get(0).trim();
        String lastNameParam = queryStringDecoder.parameters().getOrDefault("lastname", List.of("")).get(0).trim();

        int page = Integer.parseInt(pageParam);
        int size = Integer.parseInt(sizeParam);


        if (nameParam.isBlank() && !(lastNameParam.isBlank())) {
            return queryService.getPageByLastName(page, size, lastNameParam).flatMap(pageDTO -> {
                byte[] bytes = employeePageQueryModelSerializer.serialize(pageDTO);
                res.status(200);
                employeePageQueryModelSerializer.contentType(res);
                return res.sendByteArray(Mono.just(bytes)).then();
            }).doOnError(error -> {
                error.printStackTrace();
                        System.out.println("Error -------> " + error.getClass());
                    })
                    .onErrorResume(error -> {
                if (error instanceof LastnameInvalidCharactersException) {
                    return res.status(400).sendString(Mono.just(error.getMessage())).then();
                }
                return res.status(500).sendString(Mono.just("Internal Server Error")).then();
            });
        } else if (!(nameParam.isBlank()) && lastNameParam.isBlank()){
            return queryService.getPageByName(page, size, nameParam)
                    .flatMap(pageDTO -> {
                        byte[] bytes = employeePageQueryModelSerializer.serialize(pageDTO);
                        res.status(200);
                        employeePageQueryModelSerializer.contentType(res);
                        return res.sendByteArray(Mono.just(bytes)).then();
                    })
                    .onErrorResume(error -> {
                        if (error instanceof FirstnameInvalidCharactersException){
                            return res.status(400).sendString(Mono.just(error.getMessage())).then();
                        }
                        return res.status(500).sendString(Mono.just("Internal Server Error")).then();
                    });
        }
        return queryService.getPage(page, size)
                .flatMap(pageDTO -> {
                    byte[] bytes = employeePageQueryModelSerializer.serialize(pageDTO);
                    res.status(200);
                    employeeQueryModelSerializer.contentType(res);
                    return res.sendByteArray(Mono.just(bytes)).then();
                })
                .onErrorResume(error -> {
                    if (error instanceof EntityInvalidArgumentException){
                        return res.status(400).sendString(Mono.just(error.getMessage())).then();
                    }
                    return res.status(500).sendString(Mono.just("Internal Server Error")).then();
                });
    }

    public Mono<Void> search(HttpServerRequest req, HttpServerResponse res) {

        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(req.uri());

        // Αν δεν δωσει ο χρηστης
        String pageParam = queryStringDecoder.parameters().getOrDefault("page", List.of("0")).get(0);
        if(pageParam.isBlank()) {
            pageParam = "0";
        }

        String sizeParam = queryStringDecoder.parameters().getOrDefault("size", List.of("10")).get(0);
        if(sizeParam.isBlank()) {
            sizeParam = "10";
        }

        int page = Integer.parseInt(pageParam);
        int size = Integer.parseInt(sizeParam);

        return req.receive()
                .aggregate()
                .asByteArray()
                .defaultIfEmpty(new byte[0])
                .flatMap(bytes ->{
                    if (bytes.length == 0){
                        return queryService.getPage(page, size);
                    }
                    return Mono.fromCallable(() ->
                            searchRequestDeserializer.deserialize(bytes))
                            .flatMap(result -> queryService.getPaginatedSearchEmployees(result, page, size));
                })
                .flatMap(pageDTO -> {
                            byte[] bytes = employeePageQueryModelSerializer.serialize(pageDTO);
                            res.status(200);
                            employeePageQueryModelSerializer.contentType(res);
                            return res.sendByteArray(Mono.just(bytes)).then();
            }).doOnError(error -> {
                    error.printStackTrace();
                    System.out.println("ERROR TYPE: " + error.getClass());
            })
            .onErrorResume(error -> {
                if (    error instanceof EntityInvalidArgumentException ||
                        error instanceof SearchValueEmptyException ||
                        error instanceof SearchValueCharacterLengthException ||
                        error instanceof InvalidSearchValueException){
                    return res.status(400).sendString(Mono.just(error.getMessage())).then();
                }
                return res.status(500).sendString(Mono.just("Internal Server Error")).then();
            });
    }

    public void employeeRoutes(HttpServerRoutes routes) {
        routes
                .get("/test", this::getAllEmployees)
                .get("/employees", this::getPage) // Get all employees με pagination και default τιμες page = 0, size = 10
                .get("/employees/{id}",this::getEmployeeById)
                .delete("/employees/{id}", this::deleteEmployeeById)
                .post("/employees", this::saveEmployee)
                .put("/employees/{id}", this::updateEmployee)
                .post("/employees/search", this::search);
    }
}