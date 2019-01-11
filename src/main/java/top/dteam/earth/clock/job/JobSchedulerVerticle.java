package top.dteam.earth.clock.job;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import top.dteam.earth.clock.MainVerticle;
import top.dteam.earth.clock.NamedQuery;
import top.dteam.earth.clock.config.ClockConfiguration;
import top.dteam.earth.clock.utils.PgUtils;

public class JobSchedulerVerticle extends AbstractVerticle {

    private static final long DELAY = 10000;
    private static final long SLEEP_FOR_A_WHILE = 60000;
    private static final long INTERVAL = 60000;
    private long minDelay = 0;

    PgPool pgPool;
    PgUtils pgUtils;
    ClockConfiguration configuration;

    @Override
    public void start() {
        configuration = ClockConfiguration.getInstance();
        minDelay = configuration.minDelayByTopic();
        pgPool = PgClient.pool(vertx, configuration.pgPool());
        pgUtils = new PgUtils(pgPool);

        vertx.setTimer(DELAY, this::pollJobs);
        vertx.setPeriodic(INTERVAL, this::resetUnfinishedJobs);
    }

    @Override
    public void stop() {
        if (pgPool != null) {
            pgPool.close();
        }
    }

    private void pollJobs(long tid) {
        pgUtils.simpleSql(NamedQuery.unprocessedJob(configuration.limit()), this::processJobs);
        vertx.cancelTimer(tid);
    }

    private void resetUnfinishedJobs(long tid) {
        pgUtils.execute(NamedQuery.resetUnfinishedJob());
    }

    private void processJobs(PgRowSet rowSet) {
        boolean hasCallback = false;
        for (Row row : rowSet) {
            hasCallback = startJob(row) || hasCallback;
        }
        pollNext(rowSet.rowCount() < configuration.limit() && !hasCallback);
    }

    private boolean startJob(Row row) {
        String topic = row.getString("topic");
        boolean hasCallback = ((JsonObject) row.getJson("body")).containsKey("callback");
        vertx.setTimer(configuration.delayByTopic(topic), tid -> {
            MainVerticle.jobHandlers.get(topic).handle(row);
            vertx.cancelTimer(tid);
        });
        return hasCallback;
    }

    private void pollNext(boolean sleepForaWhile) {
        if (sleepForaWhile) {
            vertx.setTimer(SLEEP_FOR_A_WHILE, this::pollJobs);
        } else {
            vertx.setTimer(minDelay * 2, this::pollJobs);
        }
    }

}
