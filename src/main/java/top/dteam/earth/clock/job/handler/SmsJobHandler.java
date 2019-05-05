package top.dteam.earth.clock.job.handler;

import io.reactiverse.pgclient.Row;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import top.dteam.earth.clock.job.AbstractJobHandler;
import top.dteam.earth.clock.sms.ShortMessage;
import top.dteam.earth.clock.utils.HttpUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class SmsJobHandler extends AbstractJobHandler {

    private static final String TOPIC_NAME = "SMS";
    private transient final HttpClient client;
    private transient final Map<String, ?> smsConfig;

    public SmsJobHandler(Vertx vertx) {
        super(vertx);
        this.smsConfig = configuration.topicConfig(TOPIC_NAME);
        this.client = HttpUtils.httpsClient(vertx, (String) smsConfig.get("key"), (String) smsConfig.get("password"), false);
    }

    @Override
    protected void process(Row row, BiConsumer<Row, JsonObject> successHandler, BiConsumer<Row, JsonObject> failureHandler) {
        long id = row.getLong("id");
        List<String> phones = Collections.singletonList(row.getString("username"));
        String notificationType = row.getString("notification_type");
        JsonObject params = (JsonObject) row.getJson("params").value();
        String message = row.getString("message");

        @SuppressWarnings("unchecked")
        Map<String, String> templateCodeMap = (Map<String, String>) smsConfig.get("TemplateCode");

        ShortMessage shortMessage = new ShortMessage(id
                , phones
                , templateCodeMap.get(notificationType)
                , params
                , (String) smsConfig.get("SignName")
                , message);

        client.postAbs((String) smsConfig.get("BarnUrl"))
                .setChunked(true)
                .putHeader("content-type", "application/json")
                .handler(HttpUtils.successHandler(res -> successHandler.accept(row, new JsonObject().put("success", true))))
                .exceptionHandler(throwable -> failureHandler.accept(row, new JsonObject().put("success", false).put("message", throwable.getMessage())))
                .end(shortMessage.toString());
    }

    @Override
    protected String topic() {
        return TOPIC_NAME;
    }

}
