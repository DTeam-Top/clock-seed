package top.dteam.earth.clock.utils;

import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.PgTransaction;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.slf4j.Logger;

public class RollbackOnFailed implements Handler<AsyncResult<PgRowSet>> {

    private String errors;
    private Logger logger;
    private PgTransaction tx;

    public RollbackOnFailed(String errors, Logger logger, PgTransaction tx) {
        this.errors = errors;
        this.logger = logger;
        this.tx = tx;
    }

    @Override
    public void handle(AsyncResult<PgRowSet> asyncResult) {
        if (asyncResult.failed()) {
            logger.error(errors, asyncResult.cause());
            tx.rollback();
        }
    }
}
