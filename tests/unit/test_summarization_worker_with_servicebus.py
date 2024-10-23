# tests/test_summarization_worker_with_servicebus.py

import unittest
from multiprocessing import Manager
from unittest.mock import patch
from summarization_service.summarization_worker import SummarizationProcessingWorker
from shared.abstract_processing import ProcessingWorkerConfiguration
from azure.servicebus import ServiceBusMessage

class TestSummarizationProcessingWorkerWithMockedServiceBus(unittest.TestCase):

    @patch('openai.Completion.create')
    @patch('azure.servicebus.ServiceBusReceiver.receive_messages')
    @patch('azure.servicebus.ServiceBusSender.send_messages')
    @patch('azure.servicebus.ServiceBusReceiver.complete_message')
    def test_summarization_worker_with_mocked_servicebus(self, mock_complete_message, mock_send_messages, mock_receive_messages, mock_openai_create):
        manager = Manager()
        log_queue = manager.Queue()

        mock_openai_create.return_value = {'choices': [{'text': 'This is a mocked summary.'}]}
        mock_receive_messages.return_value = [ServiceBusMessage(body="Test input text to summarize", message_id="1234")]

        config = ProcessingWorkerConfiguration("input-queue", "output-queue", "dummy_connection_string")
        worker = SummarizationProcessingWorker(worker_id=1, config=config, log_queue=log_queue, stats_dict={})
        worker.run()

        mock_receive_messages.assert_called_once()
        mock_complete_message.assert_called_once()
        mock_send_messages.assert_called_once()
        mock_openai_create.assert_called_once()

if __name__ == '__main__':
    unittest.main()
