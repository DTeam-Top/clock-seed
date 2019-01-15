package top.dteam.earth.clock.job;

import io.reactiverse.pgclient.*;
import io.reactiverse.pgclient.data.Json;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.dteam.earth.clock.NamedQuery;
import top.dteam.earth.clock.config.ClockConfiguration;
import top.dteam.earth.clock.utils.PgUtils;

import java.util.function.BiConsumer;

public abstract class AbstractJobHandler implements JobHandler {

    private final static Logger logger = LoggerFactory.getLogger(AbstractJobHandler.class);

    protected Vertx vertx;
    protected PgUtils pgUtils;
    protected PgPool pgPool;
    protected ClockConfiguration configuration;

    public AbstractJobHandler(Vertx vertx) {
        this.vertx = vertx;
        this.configuration = ClockConfiguration.getInstance();
        pgPool = PgClient.pool(vertx, configuration.pgPool());
        this.pgUtils = new PgUtils(pgPool);
    }

    @Override
    public void handle(Row row) {
        logger.info("Processing a job: {}", row.getLong("id"));

        process(row, this::succeed, this::fail);
    }

    abstract protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler);

    abstract protected String topic();

    // 更新结果并创建callback任务
    private void succeed(Row row, JsonObject result) {
        pgPool.begin(res -> {
            if (res.succeeded()) {
                logger.info("Handle a job({}) successfully, saving result({})", row.getLong("id"), result);

                PgTransaction tx = res.result();

                // 记录结果
                tx.query(NamedQuery.completeJob(result, "SUCCEEDED", row.getLong("id")), this::voidHandler);
                // 若需要回调，创建回调任务
                if (PgUtils.hasCallback(row)) {
                    tx.query(NamedQuery.insertCallbackJob(new JsonObject()
                                    .put("source", row.getLong("id"))
                                    .put("callback", ((JsonObject) row.getJson("body").value()).getString("callback"))
                                    .put("result", result))
                            , this::voidHandler);
                }
                tx.commit(ar -> {
                    if (ar.succeeded()) {
                        logger.info("Job({}) result({}) saved.", row.getLong("id"), result);
                    } else {
                        logger.info("Job({}) result failed, cause: {}", row.getLong("id"), ar.cause());
                    }
                });
            } else {
                logger.error("Tx for job({}) can not begin, cause: {}", row.getLong("id"), res.cause());
            }
        });
    }

    // 未到重试次数，则下次重试；否则，记录失败并结束任务
    private void fail(Row row, JsonObject error) {
        int retry = row.getInteger("retry");
        int retryOfTopic = configuration.retryByTopic(row.getString("topic"));
        if ((retry + 1) <= retryOfTopic) {
            pgUtils.execute(NamedQuery.retryJobNextTime(), Tuple.of(row.getLong("id")));
        } else {
            pgUtils.execute(NamedQuery.completeJob(), Tuple.of(Json.create(error), "FAILED", row.getLong("id")));
        }
    }

    private void voidHandler(AsyncResult<PgRowSet> result) {
        if (result.failed()) {
            logger.error("Sql execution failed, cause: {}", result.cause());
        }
    }

}
