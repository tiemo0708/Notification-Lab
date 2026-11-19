package dev.jinhyeon.notificationlab.fcmsmoke;

import android.content.Context;
import android.content.SharedPreferences;
import android.util.AtomicFile;
import com.google.firebase.messaging.FirebaseMessagingService;
import com.google.firebase.messaging.RemoteMessage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class SmokeMessagingService extends FirebaseMessagingService {
    static final String RECEIPT = "fcm_smoke_receipt";

    @Override
    public void onNewToken(String token) {
        saveToken(this, token);
    }

    @Override
    public void onMessageReceived(RemoteMessage message) {
        String id = message.getData().get("smokeId");
        if (id == null || !id.matches("[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}")) return;
        SharedPreferences receipt = getSharedPreferences(RECEIPT, MODE_PRIVATE);
        int count = id.equals(receipt.getString("smoke_id", "")) ? receipt.getInt("count", 0) + 1 : 1;
        receipt.edit().putString("smoke_id", id).putLong("received_at_ms", System.currentTimeMillis())
                .putInt("count", count).apply();
    }

    static synchronized boolean saveToken(Context context, String token) {
        AtomicFile file = new AtomicFile(new File(context.getFilesDir(), "fcm-token"));
        FileOutputStream stream = null;
        try {
            stream = file.startWrite();
            stream.write(token.getBytes(StandardCharsets.UTF_8));
            file.finishWrite(stream);
            return true;
        } catch (IOException failure) {
            file.failWrite(stream);
            return false;
        }
    }
}
