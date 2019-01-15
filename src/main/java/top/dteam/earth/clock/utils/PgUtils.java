package top.dteam.earth.clock.utils;

import io.reactiverse.pgclient.*;
import io.reactiverse.pgclient.data.Json;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PgUtils {

    private final static Logger logger = LoggerFactory.getLogger(PgUtils.class);

    private PgPool pgPool;

    public PgUtils(PgPool pgPool) {
        this.pgPool = pgPool;
    }

    public void simpleSql(String sql, Handler<PgRowSet> resolve, Handler<Throwable> reject) {
        pgPool.query(sql, ar -> {
            if (ar.succeeded()) {
                resolve.handle(ar.result());
            } else {
                reject.handle(ar.cause());
            }
        });
    }

    public void simpleSql(String sql, Handler<PgRowSet> resolve) {
        simpleSql(sql, resolve, new SimpleLoggerRejector(logger));
    }

    public void preparedSql(String sql, Tuple arguments, Handler<PgRowSet> resolve, Handler<Throwable> reject) {
        pgPool.preparedQuery(sql, arguments, ar -> {
            if (ar.succeeded()) {
                resolve.handle(ar.result());
            } else {
                reject.handle(ar.cause());
            }
        });
    }

    public void preparedSql(String sql, Tuple arguments, Handler<PgRowSet> resolve) {
        preparedSql(sql, arguments, resolve, new SimpleLoggerRejector(logger));
    }


    public void execute(String sql) {
        pgPool.query(sql, ar -> {
            if (ar.succeeded()) {
                logger.debug("SQL ({}) succeeded.", sql);
            } else {
                logger.debug("SQL ({}) failed, cause: {}", sql, ar.cause().getMessage());
            }
        });
    }

    public void execute(String sql, Tuple arguments) {
        pgPool.preparedQuery(sql, arguments, ar -> {
            if (ar.succeeded()) {
                logger.debug("SQL ({}) with arguments ({}) succeeded.", sql, arguments);
            } else {
                logger.debug("SQL ({}) with arguments ({}) failed, cause: {}", sql, arguments, ar.cause().getMessage());
            }
        });
    }

    /**
     * 事务方式执行多条SQL
     *
     * @param sqls，可以为String和Map.Entry。若为后者，key为语句（String），value为参数（Tuple）
     */
    public void batch(List sqls) {
        pgPool.begin(res -> {
            if (res.succeeded()) {
                PgTransaction tx = res.result();
                for (Object sql : sqls) {
                    if (sql instanceof String) {
                        tx.query((String) sql, new RollbackOnFailed((String) sql, logger, tx));
                    } else if (sql instanceof Map.Entry) {
                        Map.Entry entry = (Map.Entry) sql;
                        String statement = (String) entry.getKey();
                        Tuple params = (Tuple) entry.getValue();
                        tx.preparedQuery(statement, params
                                , new RollbackOnFailed(
                                        new StringBuffer("Sql (")
                                                .append(sql)
                                                .append(") with params (")
                                                .append(params)
                                                .append(") failed.").toString(), logger, tx));
                    } else {
                        logger.error("Unknown sql type: {}", sql.getClass());
                        return;
                    }
                }

                tx.commit(ar -> {
                    if (ar.succeeded()) {
                        logger.info("Transaction succeeded");
                    } else {
                        logger.error("Transaction failed ", ar.cause());
                        tx.rollback();
                    }
                });
            }
        });
    }

    public static boolean hasCallback(Row row) {
        boolean hasCallback = false;
        Json body = row.getJson("body");
        if (body != null) {
            JsonObject value = (JsonObject) body.value();
            hasCallback = !"CALLBACK".equalsIgnoreCase(row.getString("topic")) && value.containsKey("callback");
        }
        return hasCallback;
    }
}
