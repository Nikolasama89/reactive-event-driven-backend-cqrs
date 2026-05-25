package com.itsaur.sql.repository;

import com.itsaur.repository.search.SearchComponent;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.repository.CompanyQueryRepository;
import com.itsaur.sql.search.SQLFilter;
import com.itsaur.sql.search.SQLSearch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class DbCompanyQueryRepository implements CompanyQueryRepository {

    private final ConnectionFactory cf;

    public DbCompanyQueryRepository(ConnectionFactory cf){
        this.cf = cf;
    }

    @Override
    public Mono<CompanyQueryModel> getCompanyById(Long id) {
        String sql = "SELECT c.id, c.company_name, c.vat, c.address, COUNT(e.id) AS total_employees, c.created_at," +
                " c.updated_at FROM companies c LEFT JOIN employees e ON c.id = e.company_id WHERE c.id = $1 GROUP BY c.id";
        return Mono.usingWhen(Mono.from(cf.create()),
                conn -> doFindOne(conn, sql, id), Connection::close);
    }

    private Mono<CompanyQueryModel> doFindOne(Connection conn, String sql, Long id) {

        return Mono.from(conn.createStatement(sql)
                        .bind(0, id)
                        .execute())
                .flatMap(result -> Mono.from(result.map((row, meta) ->
                                new CompanyQueryModel(
                                        row.get("id", Long.class),
                                        row.get("company_name", String.class),
                                        row.get("vat", String.class),
                                        row.get("address", String.class),
                                        row.get("total_employees", Long.class),
                                        row.get("created_at", OffsetDateTime.class),
                                        row.get("updated_at", OffsetDateTime.class)
                                )
                        ))
                );
    }

    @Override
    public Flux<CompanyQueryModel> getAllCompanies() {
        String sql = "SELECT c.id, c.company_name, c.vat, c.address, COUNT(e.id) AS total_employees, c.created_at," +
                " c.updated_at FROM companies c LEFT JOIN employees e ON c.id = e.company_id GROUP BY c.id";
        return Flux.usingWhen(
                Mono.from(cf.create()),
                conn -> doFindAll(conn, sql), Connection::close);
    }

    private Flux<CompanyQueryModel> doFindAll(Connection conn, String sql) {
        return  Flux.from(conn.createStatement(sql).execute())
                .flatMap(result ->
                        result.map(this::mapRowExtra)
                );
    }

    @Override
    public Flux<CompanyQueryModel> findPage(Integer page, Integer size) {
        int offset = page * size;
        String sql = "SELECT c.id, c.company_name, c.vat, c.address, COUNT(e.id) AS total_employees, c.created_at," +
                " c.updated_at FROM companies c LEFT JOIN employees e ON c.id = e.company_id GROUP BY c.id, c.company_name, c.vat, c.address, c.created_at, c.updated_at ORDER BY c.id LIMIT $1 offset $2";
        return Flux.usingWhen(
                Mono.from(cf.create()), conn -> Flux.from(
                        conn.createStatement(sql)
                                .bind(0, size)
                                .bind(1, offset)
                                .execute()
                ).flatMap(result -> Flux.from(result.map(this::mapRowExtra)
                )), Connection::close);

    }

    @Override
    public Mono<Long> countAll() {
        String sql = "SELECT COUNT(*) FROM companies";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql).execute())
                        .flatMap(result ->
                                Mono.from(result.map((row, meta) -> row.get(0, Long.class)))),
                Connection::close);
    }

    @Override
    public Flux<CompanyQueryModel> paginatedCompanyFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {
        StringBuilder sql = new StringBuilder("SELECT DISTINCT id, company_name, vat, address, total_employees," +
                " created_at, updated_at FROM company_search_view ");

        SQLFilter filter = new SQLSearch().handle(searchComponent);

        if (filter != null && !filter.getWhereClauses().isBlank()){
            sql.append(" WHERE ").append(filter.getWhereClauses());
        }

        sql.append(" ORDER BY id ");

        int base = filter.getParams().size();

        sql.append(" LIMIT $").append(base + 1);
        sql.append(" OFFSET $").append(base + 2);

        int offset = size * page;

        return Flux.usingWhen(cf.create(), conn -> {

                    Statement statement = conn.createStatement(sql.toString());

                    int index = 0;
                    for (Object param: filter.getParams()){
                        System.out.println("bind["+ index + "] = " + param);
                        statement.bind(index++, param);
                    }
                    System.out.println("bind["+ index + "] = " + size);
                    statement.bind(index++, size);
                    System.out.println("bind["+ index + "] = " + offset);
                    statement.bind(index, offset);

                    return Flux.from(statement.execute()).flatMap(result -> result.map(this::mapRowExtra));
                },
                Connection::close);

    }


    @Override
    public Mono<Long> countViewCompanies(SearchComponent searchComponent) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(DISTINCT id) FROM company_search_view ");

        SQLFilter filter = new SQLSearch().handle(searchComponent);

        if (filter != null && !filter.getWhereClauses().isBlank()){
            sql.append(" WHERE ").append(filter.getWhereClauses());
        }

        return Mono.usingWhen(
                cf.create(),
                connection -> {
                    Statement statement = connection.createStatement(sql.toString());

                    for (int i = 0; i < filter.getParams().size(); i++) {
                        statement.bind(i, filter.getParams().get(i));
                    }

                    return Mono.from(statement.execute())
                            .flatMap(result -> Mono.from(result.map((row, meta) ->
                                    row.get(0, Long.class)
                            )));
                },
                Connection::close
        );
    }

    private CompanyQueryModel mapRowExtra(Row row, Object meta) {
        CompanyQueryModel companyQueryModel = new CompanyQueryModel();
        companyQueryModel.setId(row.get("id", Long.class));
        companyQueryModel.setCompanyName(row.get("company_name", String.class));
        companyQueryModel.setVat(row.get("vat", String.class));
        companyQueryModel.setAddress(row.get("address", String.class));
        companyQueryModel.setTotalEmployees(row.get("total_employees", Long.class));
        OffsetDateTime createdAt = row.get("created_at", OffsetDateTime.class);
        OffsetDateTime updatedAt = row.get("updated_at", OffsetDateTime.class);
        companyQueryModel.setCreatedAt(createdAt != null ? createdAt : OffsetDateTime.now());
        companyQueryModel.setUpdatedAt(updatedAt != null ? updatedAt : OffsetDateTime.now());
        return companyQueryModel;
    }
}