from http.client import RemoteDisconnected
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock
from uuid import UUID

import send_smoke


class SendSmokeTest(unittest.TestCase):
    project = "fixture-project"
    smoke_id = UUID("1192dd4b-3538-443a-9389-de7e776555a2")
    token = "synthetic_registration_token_for_test"
    credential = "synthetic_oauth_token"

    def request(self, status=200, response=b'{"name":"projects/fixture-project/messages/0:example%id"}', send=False):
        class Response(io.BytesIO):
            pass

        message = Response(response)
        message.status = status
        connection = Mock()
        connection.getresponse.return_value = message
        factory = Mock(return_value=connection)
        report = send_smoke.submit(self.project, self.token, self.credential, self.smoke_id, send=send, connect=factory)
        factory.assert_called_once_with("fcm.googleapis.com", timeout=10)
        connection.request.assert_called_once()
        connection.close.assert_called_once()
        self.assertNotIn(self.token, str(report))
        self.assertNotIn(self.credential, str(report))
        method, path, payload, headers = connection.request.call_args.args
        self.assertEqual((method, path), ("POST", f"/v1/projects/{self.project}/messages:send"))
        self.assertEqual(headers["Authorization"], f"Bearer {self.credential}")
        self.assertEqual(json.loads(payload), {"validate_only": not send, "message": {
            "token": self.token, "data": {"smokeId": str(self.smoke_id)}}})
        return report

    def test_defaults_to_request_validation(self):
        self.assertEqual(self.request()["result"], "validated")

    def test_explicit_send_records_provider_acceptance(self):
        result = self.request(send=True)
        self.assertEqual(result["result"], "accepted")
        self.assertEqual(result["smokeId"], str(self.smoke_id))

    def test_rejections_and_ambiguous_results_expose_status_only(self):
        response = json.dumps({"error": self.token}).encode()
        for status, kind in ((403, "rejected"), (408, "unknown"), (503, "unknown"), (302, "unknown")):
            with self.subTest(status=status):
                result = self.request(status, response, send=True)
                self.assertEqual(result["httpStatus"], status)
                self.assertEqual(result["result"], kind)

    def test_missing_or_untrusted_id_is_ambiguous(self):
        for response in (b"{}", b"[1]", b"invalid json", b" " * 8193,
                         b'{"name":"projects/unrelated-project/messages/123"}'):
            with self.subTest(response_size=len(response)):
                self.assertEqual(self.request(response=response)["result"], "unknown")

    def test_lost_response_makes_no_second_send(self):
        connection = Mock()
        connection.getresponse.side_effect = RemoteDisconnected(self.credential)
        result = send_smoke.submit(self.project, self.token, self.credential, self.smoke_id,
                                   send=True, connect=Mock(return_value=connection))
        connection.request.assert_called_once()
        self.assertEqual(result["result"], "unknown")
        self.assertNotIn(self.credential, str(result))

    def test_token_file_accepts_one_token_and_rejects_extra_lines(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "device-token"
            path.write_text(self.token + "\n")
            self.assertEqual(send_smoke.read_token(path), self.token)
            for invalid in (self.token + "\n" + self.token, self.token.ljust(4099)):
                path.write_text(invalid)
                with self.assertRaises(ValueError):
                    send_smoke.read_token(path)

    def test_project_validation_precedes_connection(self):
        factory = Mock()
        with self.assertRaises(ValueError):
            send_smoke.submit("bad/path", self.token, self.credential, self.smoke_id, connect=factory)
        factory.assert_not_called()
