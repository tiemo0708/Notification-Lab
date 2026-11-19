package dev.jinhyeon.notificationlab.fcmsmoke;

import android.app.Activity;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.widget.LinearLayout;
import android.widget.TextView;
import com.google.firebase.FirebaseApp;
import com.google.firebase.messaging.FirebaseMessaging;
import java.time.Instant;

public final class MainActivity extends Activity {
    private TextView receiptView;
    private SharedPreferences receipt;
    private final SharedPreferences.OnSharedPreferenceChangeListener update = (preferences, key) -> showReceipt();

    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        LinearLayout layout = new LinearLayout(this);
        layout.setOrientation(LinearLayout.VERTICAL);
        layout.setFitsSystemWindows(true);
        int padding = Math.round(32 * getResources().getDisplayMetrics().density);
        layout.setPadding(padding, padding, padding, padding);
        TextView title = new TextView(this);
        title.setText(R.string.screen_title);
        title.setTextSize(22);
        layout.addView(title);
        TextView registration = new TextView(this);
        registration.setText(R.string.registration_pending);
        layout.addView(registration);
        receiptView = new TextView(this);
        layout.addView(receiptView);
        setContentView(layout);
        receipt = getSharedPreferences(SmokeMessagingService.RECEIPT, MODE_PRIVATE);

        if (FirebaseApp.getApps(this).isEmpty()) {
            registration.setText(R.string.project_required);
            return;
        }
        FirebaseMessaging.getInstance().getToken().addOnCompleteListener(this, task -> {
            boolean saved = task.isSuccessful() && SmokeMessagingService.saveToken(this, task.getResult());
            registration.setText(saved ? R.string.registration_ready : R.string.registration_failed);
        });
    }

    @Override
    protected void onStart() {
        super.onStart();
        receipt.registerOnSharedPreferenceChangeListener(update);
        showReceipt();
    }

    @Override
    protected void onStop() {
        receipt.unregisterOnSharedPreferenceChangeListener(update);
        super.onStop();
    }

    private void showReceipt() {
        String id = receipt.getString("smoke_id", "");
        if (id.isEmpty()) {
            receiptView.setText(R.string.receipt_pending);
            return;
        }
        String receivedAt = Instant.ofEpochMilli(receipt.getLong("received_at_ms", 0)).toString();
        receiptView.setText(getString(R.string.receipt, id, receivedAt, receipt.getInt("count", 0)));
    }
}
