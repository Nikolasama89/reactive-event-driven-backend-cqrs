package com.itsaur.elastic.repository;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.EmployeeQueryRepository;
import com.itsaur.elastic.search.ElasticSearchEmployeeQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class ElasticEmployeeQueryRepository implements EmployeeQueryRepository {

    private final Logger logger = LoggerFactory.getLogger(ElasticEmployeeQueryRepository.class );
    private final ElasticsearchAsyncClient client;
    private final ElasticSearchEmployeeQueryBuilder queryBuilder;

    public ElasticEmployeeQueryRepository(ElasticsearchAsyncClient client, ElasticSearchEmployeeQueryBuilder queryBuilder) {
        this.client = client;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public Mono<EmployeeQueryModel> getEmployeeById(Long id) {

//        return Mono.fromFuture(() -> client.get(g -> g.index("employees").id(String.valueOf(id)), EmployeeQueryModel.class))
//                .flatMap(response -> {
//                    if (response.found()){
//                        return Mono.just(response.source());
//                    }
//                    return Mono.empty();
//                });

        GetRequest request = GetRequest.of(g -> g
                .index("employees")
                .id(String.valueOf(id)));

        return Mono.fromFuture(() -> client.get(request, EmployeeQueryModel.class))
                .filter(GetResult::found)
                .map(GetResult::source);
    }

    @Override
    public Flux<EmployeeQueryModel> getAllEmployees() {
        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .query(q -> q.matchAll(m -> m)));

        return Mono.fromFuture(() -> client
                        .search(request, EmployeeQueryModel.class))
                .flatMapMany(resp -> Flux.fromIterable(resp.hits().hits()))
                .doOnNext(hit -> logger.info("Hit raw: {}", hit))
                .map(Hit::source)
                .doOnNext(src -> logger.info("Mapped source: {}", src));
    }

    @Override
    public Flux<EmployeeQueryModel> findPage(Integer page, Integer size) {

        int offset = page * size;
        SearchRequest request = SearchRequest.of(s -> s.index("employees").from(offset).size(size));

        return Mono.fromFuture(() -> client
                        .search(request, EmployeeQueryModel.class))
                .flatMapMany(resp ->
                        Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Mono<Long> countAll() {

        return Mono.fromFuture(() -> client.count(c -> c.index("employees")
                        .query(q -> q.matchAll(m -> m))))
                .map(CountResponse::count);
    }

    @Override
    public Flux<EmployeeQueryModel> findPageByLastName(Integer page, Integer size, String lastName) {

        int offset = page * size;
        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .from(offset)
                .size(size)
                .query(q -> q.bool(b -> {
                            if (lastName != null && !lastName.isBlank()) {
                                b.filter(f -> f.match(m -> m.field("lastname").query(lastName)
                                        )
                                );
                            }
                            return b;
                        })
                )
        );

        return Mono.fromFuture(client.search(request, EmployeeQueryModel.class))
                .flatMapMany(resp -> Flux
                        .fromIterable(resp.hits().hits())).map(Hit::source);
    }

    @Override
    public Flux<EmployeeQueryModel> findPageByFirstName(Integer page, Integer size, String firstName) {

        int offset = page * size;
        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .from(offset)
                .size(size)
                .query(q -> q.bool(b -> {
                            if (firstName != null && !firstName.isBlank()) {
                                b.filter(f -> f.match(m -> m.field("firstname").query(firstName)
                                        )
                                );
                            }
                            return b;
                        })
                )
        );

        return Mono.fromFuture(client.search(request, EmployeeQueryModel.class))
                .flatMapMany(resp -> Flux
                        .fromIterable(resp.hits().hits())).map(Hit::source);
    }

    @Override
    public Mono<Long> countAllByFirstName(String firstName) {
        CountRequest request = CountRequest.of(c -> c
                .index("employees")
                .query(q -> {
                    if (firstName == null || firstName.isBlank()) {
                        return q.matchAll(m ->m);
                    }
                    return q.match(m -> m
                            .field("firstname")
                            .query(firstName)
                    );
                })
        );
        return Mono.fromFuture(client.count(request))
                .map(CountResponse::count);
    }

    @Override
    public Mono<Long> countAllByLastName(String lastName) {
        CountRequest request = CountRequest.of(c -> c
                .index("employees")
                .query(q -> {
                    if (lastName == null || lastName.isBlank()) {
                        return q.matchAll(m -> m);
                    }
                    return q.match(m -> m
                            .field("lastname")
                            .query(lastName)
                    );
                })
        );
        return Mono.fromFuture(client.count(request))
                .map(CountResponse::count);
    }

    @Override
    public Flux<EmployeeQueryModel> filterQuery(SearchComponent searchComponent) {
        Query query = queryBuilder.build(searchComponent);

        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .query(query)
        );

        return Mono.fromFuture(() -> client.search(request, EmployeeQueryModel.class))
                .flatMapMany(resp -> Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Flux<EmployeeQueryModel> paginatedFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {
        int offset = page * size;
        Query query = queryBuilder.build(searchComponent);

        SearchRequest request = SearchRequest.of(s -> s
                .index("employees")
                .from(offset)
                .size(size)
                .query(query)
        );

        return Mono.fromFuture(() -> client.search(request, EmployeeQueryModel.class))
                .flatMapMany(resp -> Flux.fromIterable(resp.hits().hits()))
                .map(Hit::source);
    }

    @Override
    public Mono<Long> countViewEmployees(SearchComponent searchComponent) {
        Query query = queryBuilder.build(searchComponent);

        CountRequest request = CountRequest.of(c -> c
                .index("employees")
                .query(query)
        );

        return Mono.fromFuture(() -> client.count(request))
                .map(CountResponse::count);
    }
}
