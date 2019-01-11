package top.dteam.earth.clock.job.handler;

import io.reactiverse.pgclient.Row;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import top.dteam.earth.clock.job.AbstractJobHandler;
import top.dteam.earth.clock.utils.HttpUtils;

import java.util.function.BiConsumer;

public class CallbackJobHandler extends AbstractJobHandler {

    HttpClient httpClient;

    public CallbackJobHandler(Vertx vertx) {
        super(vertx);
        this.httpClient = vertx.createHttpClient();
    }

    @Override
    protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
        JsonObject result = ((JsonObject) row.getJson("body").value()).getJsonObject("result");
        String callback = result.getString("callback");
        httpClient.postAbs(configuration.callbackRoot() + callback)
                .setChunked(true)
                .putHeader("content-type", "application/json")
                .handler(HttpUtils.successHandler(res -> successHandler.accept(row, new JsonObject().put("success", true))))
                .exceptionHandler(throwable -> failureHandler.accept(row, new JsonObject().put("success", false).put("message", throwable.getMessage())))
                .end(result.toString());
    }

    @Override
    protected String topic() {
        return "CALLBACK";
    }

}
