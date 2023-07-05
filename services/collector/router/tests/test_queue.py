import unittest
from unittest.mock import patch, MagicMock
from ..event_router import route_event, send_message_to_sqs, EventTypes
import uuid

class TestEventRouter(unittest.TestCase):
    def setUp(self):
        self.event = { 
            "type": "test",
            "data": {
                "test": "data"
            }
        }

        self.resource_id = uuid.uuid4().hex
        self.transaction_id = uuid.uuid4().hex

    def test_route_event_no_event(self):
        self.assertEqual(route_event(), 500)

    def test_route_event_no_resource_id(self):
        self.assertEqual(route_event(event=self.event), 500)

    def test_route_event_invalid_event_type(self):
        self.event["type"] = "invalid"
        self.assertEqual(route_event(event=self.event, resource_id=self.resource_id), 400)

    @patch("services.collector.router.event_router.send_message_to_sqs")
    def test_route_event_valid_event_type(self, mock_send_message_to_sqs):
        self.event["type"] = EventTypes.sampletype1.value
        self.assertEqual(route_event(event=self.event, resource_id=self.resource_id), 200)

    @patch("services.collector.router.event_router.boto3")
    def test_send_message_to_sqs(self, mock_boto3):
        mock_boto3.Session.return_value.client.return_value.send_message.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        self.assertEqual(send_message_to_sqs(uri=EventTypes.sampletype1.value, event=self.event, resource_id=self.resource_id, transaction_id=self.transaction_id), 200)

    @patch("services.collector.router.event_router.boto3")
    def test_send_message_to_sqs_error(self, mock_boto3):
        mock_boto3.Session.return_value.client.return_value.send_message.return_value = {"Error": "Error"}
        self.assertEqual(send_message_to_sqs(uri=EventTypes.sampletype1.value, event=self.event, resource_id=self.resource_id, transaction_id=self.transaction_id), 500)

    @patch("services.collector.router.event_router.boto3")
    def test_send_message_to_sqs_http_error(self, mock_boto3):
        mock_boto3.Session.return_value.client.return_value.send_message.return_value = {"ResponseMetadata": {"HTTPStatusCode": 400}}
        self.assertEqual(send_message_to_sqs(uri=EventTypes.sampletype1.value, event=self.event, resource_id=self.resource_id, transaction_id=self.transaction_id), 500)

if __name__ == "__main__":
    unittest.main()
