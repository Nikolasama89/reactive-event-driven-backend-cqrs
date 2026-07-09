package com.training.handler;

import com.training.dto.*;
import com.training.exceptions.*;
import com.training.model.Company;
import com.training.model.CompanyQueryModel;
import com.training.repository.search.SearchComponent;
import com.training.serializer.*;
import com.training.service.CompanyQueryService;
import com.training.service.CompanyService;
import io.netty.handler.codec.http.QueryStringDecoder;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.HttpServerRoutes;

import java.util.List;

public class CompanyHandler {

    private final CompanyService companyService;
    private final CompanyQueryService companyQueryService;
    private final Serializer<Company> companySerializer;
    private final Deserializer<CompanyCreateDTO> deserializerCreate;
    private final Deserializer<CompanyUpdateDTO> deserializerUpdate;
    private final Serializer<CreateResponseDTO> responseSerializer;
    private final Serializer<CompanyQueryModel> companyQueryModelSerializer;
    private final Serializer<List<CompanyQueryModel>> companyQueryListSerializer;
    private final Serializer<PageDTO<CompanyQueryModel>> companyQueryPageSerializer;
    private final Deserializer<SearchComponent> searchRequestDeserializer;


    public CompanyHandler(
            CompanyService companyService,
            CompanyQueryService companyQueryService,
            Serializer<Company> companySerializer,
            Deserializer<CompanyCreateDTO> deserializerCreate,
            Deserializer<CompanyUpdateDTO> deserializerUpdate,
            Serializer<CreateResponseDTO> responseSerializer,
            Serializer<CompanyQueryModel> companyQueryModelSerializer,
            Serializer<List<CompanyQueryModel>> companyQueryListSerializer,
            Serializer<PageDTO<CompanyQueryModel>> companyQueryPageSerializer,
            Deserializer<SearchComponent> searchRequestDeserializer
    ) {
        this.companyService = companyService;
        this.companyQueryService = companyQueryService;
        this.companySerializer = companySerializer;
        this.deserializerCreate = deserializerCreate;
        this.deserializerUpdate = deserializerUpdate;
        this.responseSerializer = responseSerializer;
        this.companyQueryModelSerializer = companyQueryModelSerializer;
        this.companyQueryListSerializer = companyQueryListSerializer;
        this.companyQueryPageSerializer = companyQueryPageSerializer;
        this.searchRequestDeserializer = searchRequestDeserializer;
    }

    public Mono<Void> getAllCompanies(HttpServerRequest req, HttpServerResponse res) {
        return companyQueryService.findAllCompanies()
                .collectList()
                .flatMap(list -> {
                    byte[] bytes = companyQueryListSerializer.serialize(list);
                    return companyQueryListSerializer.contentType(res)
                            .sendByteArray(Mono.just(bytes))
                            .then();
        });
    }

    public Mono<Void> getCompanyById(HttpServerRequest req, HttpServerResponse res) {
        String paramId = req.param("id");
        if (paramId == null || paramId.isBlank()) {
            return res.status(404).sendString(Mono.just("Id can not be blank")).then();
        }
        Long id = Long.parseLong(paramId);

        return companyQueryService.findCompanyById(id).flatMap(company -> {
            byte[] bytes = companyQueryModelSerializer.serialize(company);

            return companyQueryModelSerializer.contentType(res).sendByteArray(Mono.just(bytes)).then();
        }).doOnError(error -> {
            error.printStackTrace();
                    System.out.println("Error -> " + error.getClass());
                })
                .onErrorResume(ex -> {
            if (ex instanceof CompanyNotFoundException){
                return res.status(404).sendString(Mono.just(ex.getMessage())).then();
            }
            if (ex instanceof EntityInvalidArgumentException){
                return res.status(400).sendString(Mono.just(ex.getMessage())).then();
            }
            return res.status(500).sendString(Mono.just("Internal server error")).then();
        });
    }

    public Mono<Void> deleteCompanyById(HttpServerRequest req, HttpServerResponse res) {

        String paramId = req.param("id");
        if (paramId == null || paramId.isBlank()) {
            return res.status(400).sendString(Mono.just("Id can not be empty")).then();
        }
        Long id = Long.parseLong(paramId);
        return companyService.delete(id)
                .then(res.status(204).send())
                .onErrorResume(ex -> {
                    if (ex instanceof CompanyNotFoundException) {
                        return res.status(404)
                                .sendString(Mono.just("Company with id: " + id + " not found")).then();
                    }
                    if (ex instanceof EntityInvalidArgumentException) {
                        return res.status(400).sendString(Mono.just(ex.getMessage())).then();
                    }
                    return res.status(500)
                            .sendString(Mono.just("Internal Server Error"))
                            .then();
                });
    }

    public Mono<Void> saveCompany(HttpServerRequest req, HttpServerResponse res) {
        return req.receive()
                .aggregate()
                .asByteArray()
                .map(deserializerCreate::deserialize)
                .flatMap(companyService::save)
                .flatMap(saved -> {
                    res.status(201);
                    CreateResponseDTO responseDTO = new CreateResponseDTO(saved.getId().getValue());
                    deserializerCreate.contentType(res);
                    return res.sendByteArray(Mono.just(responseSerializer.serialize(responseDTO))).then();
                })
                .onErrorResume(error -> {
                    if (error instanceof CompanyNameRequiredException ||
                            error instanceof CompanyNameInvalidCharacterLengthException ||
                            error instanceof CompanyVatRequiredException ||
                            error instanceof CompanyVatInvalidInputException ||
                            error instanceof CompanyVatAlreadyExistsException ||
                            error instanceof CompanyAddressRequiredException){
                        return res.status(400).sendString(Mono.just(error.getMessage())).then();
                    }
                    return res.status(500).sendString(Mono.just("Internal server error")).then();
                });
    }

    public Mono<Void> updateCompany(HttpServerRequest req, HttpServerResponse res) {

        return Mono.fromCallable(() -> Long.parseLong(req.param("id")))
                .flatMap(id -> req.receive()
                        .aggregate()
                        .asByteArray()
                        .map(deserializerUpdate::deserialize)
                        .map(company -> new CompanyUpdateDTO(company.getCompanyName(), company.getVat(), company.getAddress()))
                        .flatMap(updated -> companyService.update(id, updated)).then(res.status(204).send()))
                        .onErrorResume(error -> {
                            if (    error instanceof CompanyNameRequiredException ||
                                    error instanceof CompanyNameInvalidCharacterLengthException ||
                                    error instanceof CompanyVatRequiredException ||
                                    error instanceof CompanyInvalidVatLengthException ||
                                    error instanceof CompanyAddressRequiredException) {
                                return res.status(400).sendString(Mono.just(error.getMessage())).then();
                            }
                            if (error instanceof CompanyNotFoundException){
                                return res.status(404).sendString(Mono.just(error.getMessage())).then();
                            }
                            return res.status(500).sendString(Mono.just("Internal server error")).then();
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

        int page = Integer.parseInt(pageParam);
        int size = Integer.parseInt(sizeParam);

        return companyQueryService.getPage(page, size)
                .flatMap(
                pageDTO -> {
                    byte[] bytes = companyQueryPageSerializer.serialize(pageDTO);
                    res.status(200);
                    companyQueryPageSerializer.contentType(res);
                    return res.sendByteArray(Mono.just(bytes)).then();
                }).doOnError(error -> {
                    error.printStackTrace();
                    System.out.println("Error -> " + error.getClass());
                })
                .onErrorResume(error -> {
                    if (error instanceof EntityInvalidArgumentException){
                        return res.status(400).sendString(Mono.just(error.getMessage())).then();
                    }
                    return res.status(500).sendString(Mono.just("Internal server error")).then();
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
                        return companyQueryService.getPage(page, size);
                    }
                    return Mono.fromCallable(() ->
                                    searchRequestDeserializer.deserialize(bytes))
                            .flatMap(result -> companyQueryService.getPaginatedSearchCompanies(result, page, size));
                })
                .flatMap(pageDTO -> {
                    byte[] bytes = companyQueryPageSerializer.serialize(pageDTO);
                    res.status(200);
                    companyQueryModelSerializer.contentType(res);
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

    public void companyRoutes(HttpServerRoutes routes) {
        routes
                .get("/check", this::getAllCompanies)
                .get("/companies", this::getPage) // Pagination default τιμες page = 0, size = 10
                .get("/companies/{id}",this::getCompanyById)
                .delete("/companies/{id}", this::deleteCompanyById)
                .post("/companies", this::saveCompany)
                .put("/companies/{id}", this::updateCompany)
                .post("/companies/search", this::search);
    }
}
