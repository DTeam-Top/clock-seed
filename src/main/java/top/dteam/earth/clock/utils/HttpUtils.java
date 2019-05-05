package top.dteam.earth.clock.utils;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PfxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {

    private final static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    public static HttpClient httpsClient(Vertx vertx, String pfxFile, String pfxPassword
            , String jksFile, String jksPassword, boolean verifyHost) {

        return vertx.createHttpClient(new HttpClientOptions()
                .setSsl(true)
                .setPfxKeyCertOptions(new PfxOptions().setPath(pfxFile).setPassword(pfxPassword))
                .setTrustStoreOptions(new JksOptions().setPath(jksFile).setPassword(jksPassword))
                .setVerifyHost(verifyHost));
    }

    public static HttpClient httpsClient(Vertx vertx, String pfxFile, String pfxPassword, boolean verifyHost) {
        return vertx.createHttpClient(new HttpClientOptions()
                .setSsl(true)
                .setTrustAll(true)
                .setPfxKeyCertOptions(new PfxOptions().setPath(pfxFile).setPassword(pfxPassword))
                .setVerifyHost(verifyHost));
    }

    public static Handler<HttpClientResponse> successHandler(Handler<JsonObject> handler) {
        return response -> {
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                responseConsumer(response, handler);
            } else {
                responseConsumer(response, (JsonObject result) -> {
                    logger.error("request failed, status: {}, body: {}", response.statusCode(), result);
                });
            }
        };
    }

    public static void responseConsumer(HttpClientResponse response, Handler<JsonObject> handler) {
        response.bodyHandler((Buffer totalBuffer) -> {
            if (totalBuffer.length() > 0) {
                handler.handle(totalBuffer.toJsonObject());
            } else {
                logger.warn("Response body length == 0.");
            }
        });
    }
}
