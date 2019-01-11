package top.dteam.earth.clock.job;

import io.reactiverse.pgclient.*;
import io.reactiverse.pgclient.data.Json;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import top.dteam.earth.clock.NamedQuery;
import top.dteam.earth.clock.config.ClockConfiguration;
import top.dteam.earth.clock.utils.PgUtils;

import java.util.function.BiConsumer;

public abstract class AbstractJobHandler implements JobHandler {

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
        pgUtils.execute(NamedQuery.setJobProcessing(), Tuple.of(row.getLong("id")));
        process(row, this::succeed, this::fail);
    }

    abstract protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler);

    abstract protected String topic();

    // 更新结果并创建callback任务
    private void succeed(Row row, JsonObject result) {
        pgPool.begin(res -> {
            if (res.succeeded()) {
                PgTransaction tx = res.result();
                tx.preparedQuery(NamedQuery.completeJob(), Tuple.of(Json.create(result), "SUCCEEDED", row.getLong("id")), this::voidHandler);
                tx.preparedQuery(NamedQuery.insertCallbackJob(), Tuple.of(Json.create(result)), this::voidHandler);
                tx.commit();
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
            pgUtils.execute(NamedQuery.completeJob(), Tuple.of(error, "FAILED", row.getLong("id")));
        }
    }

    private void voidHandler(AsyncResult<PgRowSet> rowSet) {

    }

}
