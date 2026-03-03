package com.itsaur.repository;

import com.itsaur.dto.SearchComponent;
import com.itsaur.model.EmployeeQueryModel;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class DBEmployeeQueryRepository implements EmployeeQueryRepository {

    private final ConnectionFactory cf;

    public DBEmployeeQueryRepository(ConnectionFactory cf) {
        this.cf = cf;
    }

    @Override
    public Mono<EmployeeQueryModel> getEmployeeById(Long id) {
        String sql = """
                SELECT e.id, e.firstname, e.lastname, e.email, e.company_id, c.company_name,
                 c.address, e.created_at, e.updated_at FROM employees e INNER JOIN companies c ON e.company_id = c.id where e.id = $1""";
        return Mono.usingWhen(
                Mono.from(cf.create()),
                conn -> doFindOne(conn, sql, id), Connection::close);
    }

    private Mono<EmployeeQueryModel> doFindOne(Connection conn, String sql, Long id) {

        return Mono.from(conn.createStatement(sql).bind(0, id)
                        .execute()
                )
                .flatMap(result -> Mono.from(result
                        .map(this::mapRowExtra)));
    }

    @Override
    public Flux<EmployeeQueryModel> getAllEmployees() {
        String sql = """
                SELECT e.id, e.firstname, e.lastname, e.email, e.company_id, c.company_name,
                 c.address, e.created_at, e.updated_at FROM employees e INNER JOIN companies c ON e.company_id = c.id ORDER BY e.id
                """;
        return Flux.usingWhen(
                Mono.from(cf.create()),
                conn -> doFindAll(conn, sql), Connection::close);
    }

    private Flux<EmployeeQueryModel> doFindAll(Connection conn, String sql) {
        return  Flux.from(conn.createStatement(sql).execute())
                .flatMap(result ->
                        result.map((row, meta) -> mapRowExtra(row, meta))
                );
    }

    private EmployeeQueryModel mapRowExtra(Row row, Object meta) {
        EmployeeQueryModel employeeQueryModel = new EmployeeQueryModel();
        employeeQueryModel.setId(row.get("id", Long.class));
        employeeQueryModel.setFirstName(row.get("firstname", String.class));
        employeeQueryModel.setLastName(row.get("lastname", String.class));
        employeeQueryModel.setEmail(row.get("email", String.class));
        employeeQueryModel.setCompanyId(row.get("company_id", Long.class));
        employeeQueryModel.setCompanyName(row.get("company_name", String.class));
        employeeQueryModel.setCompanyAddress(row.get("address", String.class));
        OffsetDateTime createdAt = row.get("created_at", OffsetDateTime.class);
        OffsetDateTime updatedAt = row.get("updated_at", OffsetDateTime.class);
        employeeQueryModel.setCreatedAt(createdAt != null ? createdAt : OffsetDateTime.now());
        employeeQueryModel.setUpdatedAt(updatedAt != null ? updatedAt : OffsetDateTime.now());
        return employeeQueryModel;
    }


    @Override
    public Flux<EmployeeQueryModel> findPage(Integer page, Integer size) {
        int offset = page * size;
        String sql = "SELECT e.id, e.firstname, e.lastname, e.email, e.company_id, c.company_name, c.address," +
                " e.created_at, e.updated_at FROM employees e " +
                "INNER JOIN companies c ON e.company_id = c.id ORDER BY e.id LIMIT $1 offset $2";
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
        String sql = "SELECT COUNT(*) FROM employees";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql).execute())
                        .flatMap(result ->
                                        Mono.from(result.map((row, meta) -> row.get(0, Long.class)))),
                Connection::close);
    }

    @Override
    public Flux<EmployeeQueryModel> findPageByLastName(Integer page, Integer size, String lastName) {
        int offset = page * size;
        String sql = "SELECT e.id, e.firstname, e.lastname, e.email, e.company_id, c.company_name, c.address," +
                " e.created_at, e.updated_at FROM employees e " +
                "INNER JOIN companies c ON e.company_id = c.id WHERE e.lastname ILIKE $3 ORDER BY e.id LIMIT $1 offset $2 ";
        return Flux.usingWhen(
                Mono.from(cf.create()), conn -> Flux.from(
                        conn.createStatement(sql)
                                .bind(0, size)
                                .bind(1, offset)
                                .bind(2, lastName + "%")
                                .execute()
                ).flatMap(result -> Flux.from(result.map(this::mapRowExtra)
                )), Connection::close);
    }


    @Override
    public Flux<EmployeeQueryModel> findPageByFirstName(Integer page, Integer size, String firstName) {
        int offset = page * size;
        String sql = "SELECT e.id, e.firstname, e.lastname, e.email, e.company_id, c.company_name, c.address," +
                " e.created_at, e.updated_at FROM employees e " +
                "INNER JOIN companies c ON e.company_id = c.id WHERE e.firstname ILIKE $3 ORDER BY e.id LIMIT $1 offset $2 ";
        return Flux.usingWhen(
                Mono.from(cf.create()), conn -> Flux.from(
                        conn.createStatement(sql)
                                .bind(0, size)
                                .bind(1, offset)
                                .bind(2, firstName + "%")
                                .execute()
                ).flatMap(result -> Flux.from(result.map(this::mapRowExtra)
                )), Connection::close);
    }


    @Override
    public Mono<Long> countAllByFirstName(String firstName) {
        String sql = "SELECT COUNT(*) FROM employees WHERE firstname ILIKE $1";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql)
                                .bind(0, firstName + "%")
                                .execute())
                        .flatMap(result ->
                                Mono.from(result.map((row, meta) -> row.get(0, Long.class)))),
                Connection::close);
    }

    @Override
    public Mono<Long> countAllByLastName(String lastName) {
        String sql = "SELECT COUNT(*) FROM employees WHERE lastname ILIKE $1";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql)
                                .bind(0, lastName + "%")
                                .execute())
                        .flatMap(result ->
                                Mono.from(result.map((row, meta) -> row.get(0, Long.class)))),
                Connection::close);
    }

    public Flux<EmployeeQueryModel> filterQuery(SearchComponent searchComponent) {

        SQLSearchCompiler compiler = new SQLSearchCompiler();
        SQLFilter filter = compiler.compile(searchComponent);

        StringBuilder sql = new StringBuilder("SELECT * FROM employee_view ");
        List<String> params = new ArrayList<>();


        if(searchComponent != null) {

            sql.append(" WHERE ").append(filter.getWhereClause());
        }
        return Flux.usingWhen(
                cf.create(),
                connection -> {

                    Statement statement = connection.createStatement(String.valueOf(sql));

                    for (int i = 0; i < params.size(); i++) {
                        statement.bind(i, params.get(i));
                    }
                    return Flux.from(statement.execute())
                            .flatMap(result ->
                                    result.map(this::mapRowExtra)
                            );
                },
                Connection::close
        );
    }

    @Override
    public Flux<EmployeeQueryModel> paginatedFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {

        SQLSearchCompiler compiler = new SQLSearchCompiler();
        SQLFilter filter = compiler.compile(searchComponent);

        StringBuilder sql = new StringBuilder("SELECT * FROM employee_view ");
        List<String> params = new ArrayList<>();

        if(searchComponent != null) {

            sql.append(" WHERE ").append(filter.getWhereClause());
        }
        sql.append(" ORDER BY id");
        sql.append(" LIMIT $").append(filter.getParams().size() + 1);
        sql.append(" OFFSET $").append(filter.getParams().size() + 2);

        int offset = size * page;

        return Flux.usingWhen(cf.create(), conn -> {
            Statement statement = conn.createStatement(sql.toString());

            int index = 0;
            for (Object param: filter.getParams()){
                statement.bind(index++, param);
            }
            statement.bind(index++, size);
            statement.bind(index, offset);

            return Flux.from(statement.execute()).flatMap(result -> result.map(this::mapRowExtra));
        },
                Connection::close);
    }

    @Override
    public Mono<Long> countViewEmployees(SearchComponent searchComponent) {

        SQLSearchCompiler compiler = new SQLSearchCompiler();
        SQLFilter filter = compiler.compile(searchComponent);

        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM employee_view ");

        if (searchComponent != null) {

            sql.append(" WHERE ").append(filter.getWhereClause());
        }

        return Mono.usingWhen(
                cf.create(),
                connection -> {

                    Statement statement = connection.createStatement(sql.toString());

                    for (int i = 0; i < filter.getParams().size(); i++) {
                        statement.bind(i, filter.getParams().get(i));
                    }

                    return Mono.from(statement.execute())
                            .flatMap(result ->
                                    Mono.from(result.map((row, meta) ->
                                            row.get(0, Long.class)
                                    ))
                            );
                },
                Connection::close
        );
    }
}
