package top.dteam.earth.clock.job

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.Tuple
import io.reactiverse.pgclient.data.Json
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable
import top.dteam.earth.clock.config.ClockConfiguration
import top.dteam.earth.clock.utils.PgUtils

import java.time.LocalDateTime
import java.util.function.BiConsumer

class JobHandlerSpec extends Specification {

    @Shared
    PgPool pgPool

    @Shared
    Vertx vertx

    void setupSpec() {
        String config = """
            pgPool {
                host = System.getenv("JDBC_HOST") ?: '127.0.0.1'
                port = System.getenv("JDBC_PORT") ? System.getenv("JDBC_PORT") as int : 5432
                database = System.getenv("JDBC_DATABASE") ?: 'earth'
                user = System.getenv("JDBC_USER") ?: 'earth_admin'
                password = System.getenv("JDBC_PASSWORD") ?: 'admin'
                maxSize = 5
                cachePreparedStatements = true
            }
        """
        vertx = Vertx.vertx()
        ClockConfiguration.instance = ClockConfiguration.build(config)
        pgPool = PgClient.pool(ClockConfiguration.instance.pgPool)
    }

    void cleanupSpec() {
        pgPool.close()
        vertx.close()
    }

    void "应该能正确执行不需要回调通知的任务"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                ) { ar -> println ar.cause() }
            }
        }
        JobHandler jobHandler = new AbstractJobHandler(vertx) {
            @Override
            protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
                successHandler.accept(row, new JsonObject([success: true]))
            }

            @Override
            protected String topic() {
                'JOB'
            }
        }
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()

        when:
        sleep(300)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            jobHandler.handle(rowSet.asList()[0])
        }
        sleep(1000)
        pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
            rows.set(rowSet.asList())
        }

        then:
        rows.get().size() == 1
        rows.get()[0].getString('status') == 'SUCCEEDED'
        rows.get()[0].getJson('result').value().map == [success: true]
    }

    void "应该能正确执行需要回调通知的任务"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject([callback: 'url'])), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                ) { ar -> println ar.cause() }
            }
        }
        JobHandler jobHandler = new AbstractJobHandler(vertx) {
            @Override
            protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
                successHandler.accept(row, new JsonObject([success: true]))
            }

            @Override
            protected String topic() {
                'JOB'
            }
        }
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()

        when:
        sleep(300)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            jobHandler.handle(rowSet.asList()[0])
        }
        sleep(1000)
        pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
            rows.set(rowSet.asList())
        }

        then:
        rows.get().size() == 2
        rows.get()[0].getString('status') == 'SUCCEEDED'
        rows.get()[0].getJson('result').value().map == [success: true]
        rows.get()[1].getString('topic') == 'CALLBACK'
        rows.get()[1].getInteger('priority') == 10
        rows.get()[1].getJson('body').value().map == [source: 1, result: [success: true], callback: 'url']
        rows.get()[1].getString('status') == 'CREATED'
        rows.get()[1].getInteger('retry') == 0
    }

    void "未到重试次数的Job应该可以重试"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                ) { ar -> println ar.cause() }
            }
        }
        JobHandler jobHandler = new AbstractJobHandler(vertx) {
            @Override
            protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
                failureHandler.accept(row, new JsonObject())
            }

            @Override
            protected String topic() {
                'JOB'
            }
        }
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()

        when:
        sleep(300)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            jobHandler.handle(rowSet.asList()[0])
        }
        sleep(1000)
        pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
            rows.set(rowSet.asList())
        }

        then:
        rows.get().size() == 1
        rows.get()[0].getString('status') == 'CREATED'
        rows.get()[0].getInteger('retry') == 1
    }

    void "到重试次数的Job应该标记失败"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'CREATED', 3, LocalDateTime.now(), LocalDateTime.now())
                ) { ar -> println ar.cause() }
            }
        }
        JobHandler jobHandler = new AbstractJobHandler(vertx) {
            @Override
            protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
                failureHandler.accept(row, new JsonObject([success: false]))
            }

            @Override
            protected String topic() {
                'JOB'
            }
        }
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()

        when:
        sleep(300)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            jobHandler.handle(rowSet.asList()[0])
        }
        sleep(1000)
        pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
            rows.set(rowSet.asList())
        }

        then:
        rows.get().size() == 1
        rows.get()[0].getString('status') == 'FAILED'
        rows.get()[0].getJson('result').value().map == [success: false]
        rows.get()[0].getInteger('retry') == 3
    }

}
