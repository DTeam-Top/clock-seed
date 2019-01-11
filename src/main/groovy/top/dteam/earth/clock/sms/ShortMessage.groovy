package top.dteam.earth.clock.sms

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class ShortMessage {

    long id
    List<String> phones
    String templateCode
    JsonObject templateParam
    String freeSignName
    String message

    ShortMessage(long id, List<String> phones, String templateCode
                 , JsonObject templateParam, String freeSignName, String message) {
        this.id = id
        this.phones = phones
        this.templateCode = templateCode
        this.templateParam = templateParam
        this.freeSignName = freeSignName
        this.message = message
    }

    String toString() {
        new JsonObject()
                .put("phones", new JsonArray(phones))
                .put("templateCode", templateCode)
                .put("templateParam", templateParam.toString())
                .put("freeSignName", freeSignName)
                .put("message", message).toString()
    }

}
