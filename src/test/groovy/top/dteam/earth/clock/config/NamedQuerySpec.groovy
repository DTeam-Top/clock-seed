package top.dteam.earth.clock.config

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.Tuple
import io.reactiverse.pgclient.data.Json
import io.vertx.core.json.JsonObject
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable
import top.dteam.earth.clock.NamedQuery
import top.dteam.earth.clock.utils.PgUtils

import java.time.LocalDateTime

class NamedQuerySpec extends Specification {

    @Shared
    PgPool pgPool

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
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)
        pgPool = PgClient.pool(clockConfiguration.pgPool())
    }

    void cleanupSpec() {
        pgPool.close()
    }

    void 'unprocessedJob应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'SUCCEEDED', 0, LocalDateTime.now().minusMinutes(15), LocalDateTime.now())
                        , Tuple.of('SMS', 5, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now().minusMinutes(10), LocalDateTime.now())
                        , Tuple.of('CALLBACK', 10, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                        , Tuple.of('SMS', 5, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(300)
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()
        BlockingVariable<Integer> rowCount = new BlockingVariable<>()
        pgUtils.simpleSql(NamedQuery.unprocessedJob(10)) { rowSet ->
            rowCount.set(rowSet.rowCount())
            rows.set(rowSet.asList())
        }

        then:
        rowCount.get() == 3
        rows.get()[0].getString('topic') == 'CALLBACK'
        rows.get()[1].getString('topic') == 'SMS'
        rows.get()[1].getLong('id') < rows.get()[2].getLong('id')
    }

    void 'resetUnfinishedJob应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'PROCESSING', 1, LocalDateTime.now(), LocalDateTime.now().minusHours(5))
                        , Tuple.of('SMS', 5, Json.create(new JsonObject()), 'PROCESSING', 3, LocalDateTime.now(), LocalDateTime.now().minusHours(5))
                        , Tuple.of('SMS', 5, Json.create(new JsonObject()), 'PROCESSING', 0, LocalDateTime.now(), LocalDateTime.now())
                        , Tuple.of('CALLBACK', 10, Json.create(new JsonObject()), 'SUCCEEDED', 0, LocalDateTime.now(), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(100)
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()
        pgUtils.simpleSql(NamedQuery.resetUnfinishedJob(4)) {
            pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
                rows.set(rowSet.asList())
            }
        }

        then:
        // 满足条件
        rows.get()[0].getString('topic') == 'SMS'
        rows.get()[0].getString('status') == 'CREATED'
        rows.get()[0].getInteger('retry') == 2
        // 重试次数超过
        rows.get()[1].getString('topic') == 'SMS'
        rows.get()[1].getString('status') == 'PROCESSING'
        rows.get()[1].getInteger('retry') == 3
        // 任务太近
        rows.get()[2].getString('topic') == 'SMS'
        rows.get()[2].getString('status') == 'PROCESSING'
        rows.get()[2].getInteger('retry') == 0
        // 已完成任务
        rows.get()[3].getString('topic') == 'CALLBACK'
        rows.get()[3].getString('status') == 'SUCCEEDED'
        rows.get()[3].getInteger('retry') == 0
    }

    void 'setJobProcessing应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'CREATED', 1, LocalDateTime.now().minusMinutes(10), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(100)
        BlockingVariable<String> status = new BlockingVariable<>()
        pgUtils.preparedSql(NamedQuery.setJobProcessing(), Tuple.of(1)) {
            pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
                status.set(rowSet.asList()[0].getString('status'))
            }
        }

        then:
        status.get() == 'PROCESSING'
    }

    void 'completeJob应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'CREATED', 1, LocalDateTime.now().minusMinutes(10), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(100)
        BlockingVariable<Row> row = new BlockingVariable<>()
        pgUtils.preparedSql(NamedQuery.completeJob(), Tuple.of(Json.create(new JsonObject([success: 'true'])), 'SUCCEEDED', 1)) {
            pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
                row.set(rowSet.asList()[0])
            }
        }

        then:
        row.get().getJson('result').value().map == [success: 'true']
        row.get().getString('status') == 'SUCCEEDED'
    }

    void 'retryJobNextTime应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'PROCESSING', 1, LocalDateTime.now().minusMinutes(10), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(100)
        BlockingVariable<Row> row = new BlockingVariable<>()
        pgUtils.preparedSql(NamedQuery.retryJobNextTime(), Tuple.of(1)) {
            pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
                row.set(rowSet.asList()[0])
            }
        }

        then:
        row.get().getString('status') == 'CREATED'
        row.get().getLong('retry') == 2
    }

    void 'insertCallbackJob应该正确'() {
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
                pgPool.preparedBatch(''' insert into myjob 
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('SMS', 5, Json.create(new JsonObject()), 'SUCCEEDED', 1, LocalDateTime.now().minusMinutes(10), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }

        when:
        sleep(100)
        BlockingVariable<Row> row = new BlockingVariable<>()
        pgUtils.preparedSql(NamedQuery.insertCallbackJob(), Tuple.of(Json.create(new JsonObject([source: 1, callback: '', result: [success: true]])))) {
            pgUtils.simpleSql('select * from myjob where id = 2') { rowSet ->
                row.set(rowSet.asList()[0])
            }
        }

        then:
        row.get().getString('topic') == 'CALLBACK'
        row.get().getLong('priority') == 10
        row.get().getJson('body').value().map == [source: 1, callback: '', result: [success: true]]
        row.get().getString('status') == 'CREATED'
    }

}
