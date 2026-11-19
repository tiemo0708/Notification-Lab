# Android 기기 한 대의 FCM 수신 시험

시험 앱 한 인스턴스에 합성 데이터 메시지 한 건을 보낸다. 발송 도구가 기록한 `smokeId`와 Android 앱이 수신한 `smokeId`를 대조한다. 결과는 FCM 요청 검증, FCM 수락, 앱 콜백 수신으로 나눠 기록한다. 이 시험에서는 앱 화면으로 콜백을 확인한다. Notification-Lab의 Worker에서 FCM을 호출하는 경로와 OS의 알림 배너 표시는 별도 검증 범위다. [FCM 메시지 전송](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages/send) · [Android 수신 처리](https://firebase.google.com/docs/cloud-messaging/android/receive-messages)

## 시험 환경 연결

1. 전용 Firebase 프로젝트를 Spark 요금제에 준비한다. Firebase 콘솔의 Android 앱 등록에 패키지 `dev.jinhyeon.notificationlab.fcmsmoke`를 입력한다. 다운로드한 `google-services.json`은 `tools/fcm-smoke/android/`에 둔다. 등록 토큰과 ADC 자격 증명은 작업 환경에 로컬 파일로 둔다. [Android 앱 등록](https://firebase.google.com/docs/android/setup) · [Firebase 요금제](https://firebase.google.com/docs/projects/billing/firebase-pricing-plans)
2. Google Play가 설치된 Android 8 이상 테스트 기기나 Google APIs 시스템 이미지의 에뮬레이터 한 대를 선택한다. 아래 `FCM_ANDROID_SERIAL`은 `adb devices`에서 선택한 기기의 값이다. [FCM과 Google Play 환경](https://firebase.google.com/docs/cloud-messaging/android/get-started)
3. 전용 프로젝트에서 FCM API를 사용하고, `cloudmessaging.messages.create` 권한이 있는 로컬 Google Application Default Credentials를 준비한다. 기존 서비스 계정을 사용할 때는 프로젝트의 IAM 설정과 실제 대상 프로젝트를 확인한다. [FCM HTTP v1 인증](https://firebase.google.com/docs/cloud-messaging/send/v1-api) · [FCM IAM 권한](https://docs.cloud.google.com/iam/docs/roles-permissions/firebasecloudmessaging)

```sh
./gradlew --no-daemon --console=plain -p tools/fcm-smoke/android :assembleDebug
adb -s "$FCM_ANDROID_SERIAL" install -r tools/fcm-smoke/android/build/outputs/apk/debug/notification-lab-fcm-smoke-debug.apk
adb -s "$FCM_ANDROID_SERIAL" shell am start -n dev.jinhyeon.notificationlab.fcmsmoke/.MainActivity
```

앱에 `기기 등록 완료`가 표시되면 등록 토큰을 앱의 비공개 파일에서 로컬 실험 디렉터리로 옮긴다. 발송기는 이 파일 경로를 읽는다.

```sh
umask 077
mkdir -p build/fcm-smoke-private build/reports/fcm-smoke
adb -s "$FCM_ANDROID_SERIAL" exec-out run-as dev.jinhyeon.notificationlab.fcmsmoke \
  cat files/fcm-token > build/fcm-smoke-private/device-token
python3 -m venv build/fcm-smoke-private/venv
build/fcm-smoke-private/venv/bin/python -m pip install -r tools/fcm-smoke/requirements.txt
```

## 한 건 발송과 대조

`FCM_PROJECT_ID`에는 등록 토큰과 같은 프로젝트의 ID를 사용한다. 먼저 `validate_only` 요청을 실행한다.

```sh
FCM_SMOKE_ID=$(python3 -c 'from uuid import uuid4; print(uuid4())')
build/fcm-smoke-private/venv/bin/python tools/fcm-smoke/send_smoke.py \
  --project "$FCM_PROJECT_ID" --token-file build/fcm-smoke-private/device-token \
  --smoke-id "$FCM_SMOKE_ID" > build/reports/fcm-smoke/validation.json
```

결과가 `validated`이고 앱에 동일 ID의 수신 기록이 생기지 않았다면 한 건을 발송한다.

```sh
build/fcm-smoke-private/venv/bin/python tools/fcm-smoke/send_smoke.py \
  --project "$FCM_PROJECT_ID" --token-file build/fcm-smoke-private/device-token \
  --smoke-id "$FCM_SMOKE_ID" --send > build/reports/fcm-smoke/send.json
```

- FCM 수락은 `send.json`의 `accepted`, HTTP 200, 공급자 메시지 이름으로 확인한다. 네트워크 또는 응답 결과가 `unknown`이면 먼저 같은 ID가 단말에 도착했는지 확인한다. 발송 도구는 한 번만 요청한다.
- 앱 화면에서 `수신 ID`가 `FCM_SMOKE_ID`와 같은지, UTC 수신 시각과 해당 ID의 `수신 횟수`가 1인지 확인한다. 필요하면 아래 명령으로 앱 콜백의 증거를 보관한다. 실물 기기인지 에뮬레이터인지도 결과에 기록한다.

```sh
adb -s "$FCM_ANDROID_SERIAL" exec-out run-as dev.jinhyeon.notificationlab.fcmsmoke \
  cat shared_prefs/fcm_smoke_receipt.xml > build/reports/fcm-smoke/receipt.xml
```

## 로컬에서 먼저 검증할 수 있는 항목

프로젝트 연결 파일 없이도 디버그 APK와 lint 결과를 만들 수 있다. 이 경우 앱은 Firebase 프로젝트 연결이 필요하다는 상태를 표시한다. 발송 도구의 모의 테스트는 등록 토큰 읽기, 기본 검증 모드, 명시적 단건 발송, 실패 응답의 비밀정보 제외와 결과 불명 시 재요청 여부를 확인한다.

```sh
./gradlew --no-daemon --console=plain -p tools/fcm-smoke/android :assembleDebug :lintDebug
python3 -m unittest discover -s tools/fcm-smoke -v
```
