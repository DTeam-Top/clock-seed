package top.dteam.earth.clock.job;

import io.reactiverse.pgclient.*;
import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.dteam.earth.clock.NamedQuery;
import top.dteam.earth.clock.config.ClockConfiguration;
import top.dteam.earth.clock.utils.PgUtils;

public class JobSchedulerVerticle extends AbstractVerticle {

    private final static Logger logger = LoggerFactory.getLogger(JobSchedulerVerticle.class);

    static long DELAY = 10000;
    static long SLEEP_FOR_A_WHILE = 60000;
    static long INTERVAL = 60000;
    private transient long minDelay;

    private transient PgPool pgPool;
    private transient PgUtils pgUtils;
    private transient ClockConfiguration configuration;

    @Override
    public void start() {
        logger.info("Starting JobSchedulerVerticle ...");

        configuration = ClockConfiguration.getInstance();
        minDelay = configuration.minDelayByTopic();
        pgPool = PgClient.pool(vertx, configuration.getPgPool());
        pgUtils = new PgUtils(pgPool);

        vertx.setTimer(DELAY, this::pollJobs);
        vertx.setPeriodic(INTERVAL, this::resetUnfinishedJobs);
    }

    @Override
    public void stop() {
        logger.info("Stopping JobSchedulerVerticle ...");

        if (pgPool != null) {
            pgPool.close();
        }
    }

    private void pollJobs(long tid) {
        logger.info("Polling unprocessedJob ...");

        pgUtils.simpleSql(NamedQuery.unprocessedJob(configuration.getLimit()), this::processJobs);
        vertx.cancelTimer(tid);
    }

    private void resetUnfinishedJobs(long tid) {
        logger.info("Resetting unfinishedJob ...");

        pgUtils.execute(NamedQuery.resetUnfinishedJob(configuration.getTimeout()));
        vertx.cancelTimer(tid);
    }

    private void processJobs(PgRowSet rowSet) {
        boolean hasCallback = false;
        for (Row row : rowSet) {
            logger.info("Got a job: {}", row.getLong("id"));

            pgUtils.execute(NamedQuery.setJobProcessing(), Tuple.of(row.getLong("id")));
            hasCallback = startJob(row) || hasCallback;
        }
        pollNext(rowSet.rowCount() < configuration.getLimit() && !hasCallback);
    }

    private boolean startJob(Row row) {
        String topic = row.getString("topic");
        vertx.setTimer(configuration.delayByTopic(topic), (Long tid) -> {
            logger.info("Starting a job: {}", row.getLong("id"));

            configuration.getJobHandler(topic).handle(row);
            vertx.cancelTimer(tid);
        });
        return PgUtils.hasCallback(row);
    }

    private void pollNext(boolean sleepForaWhile) {
        vertx.setTimer(sleepForaWhile ? SLEEP_FOR_A_WHILE : minDelay * 2, this::pollJobs);
    }

}
