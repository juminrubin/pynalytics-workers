# tests/unit/test_speech_to_text_worker.py

import unittest
from multiprocessing import Manager
from unittest.mock import patch
from speech_to_text_service.speech_to_text_worker import SpeechToTextProcessingWorker
from shared.abstract_processing import ProcessingWorkerConfiguration
from azure.servicebus import ServiceBusMessage

class TestSpeechToTextProcessingWorkerWithMockedServiceBus(unittest.TestCase):

    @patch('whisper.Model.transcribe')
    @patch('azure.servicebus.ServiceBusReceiver.receive_messages')
    @patch('azure.servicebus.ServiceBusSender.send_messages')
    @patch('azure.servicebus.ServiceBusReceiver.complete_message')
    def test_speech_to_text_worker_with_mocked_servicebus_and_whisper(self, mock_complete_message, mock_send_messages, mock_receive_messages, mock_transcribe):
        # Set up a mock response for Whisper's transcribe method
        mock_transcribe.return_value = {'text': 'This is the transcribed text.'}

        # Set up a mock response for receiving messages from the queue
        mock_receive_messages.return_value = [
            ServiceBusMessage(body="mock_audio_data", message_id="5678")
        ]

        manager = Manager()
        log_queue = manager.Queue()

        # Prepare configuration and log queue (mock or use empty dicts for simplicity in tests)
        config = ProcessingWorkerConfiguration(
            input_queue_name="input-queue",
            output_queue_name="output-queue",
            service_bus_connection_str="dummy_connection_string"
        )

        # Initialize the SpeechToTextProcessingWorker
        worker = SpeechToTextProcessingWorker(worker_id=1, config=config, log_queue=log_queue, stats_dict={})

        # Run the worker's main method (this will simulate receiving, processing, and sending messages)
        worker.run()

        # Assert that a message was received from the input queue
        mock_receive_messages.assert_called_once()

        # Assert that the Whisper model was called to transcribe the audio data
        mock_transcribe.assert_called_once_with("mock_audio_data")

        # Assert that the completion was called to complete the message in the queue
        mock_complete_message.assert_called_once()

        # Assert that a message was sent to the output queue
        mock_send_messages.assert_called_once()

    @patch('openai.Completion.create')
    @patch('whisper.Model.transcribe')
    @patch('azure.servicebus.ServiceBusReceiver.receive_messages')
    @patch('azure.servicebus.ServiceBusSender.send_messages')
    @patch('azure.servicebus.ServiceBusReceiver.complete_message')
    def test_speech_to_text_worker_with_mocked_servicebus_and_gpt(self, mock_complete_message, mock_send_messages, mock_receive_messages, mock_transcribe, mock_openai_create):
        # Set up a mock response for Whisper's transcribe method
        mock_transcribe.return_value = {'text': 'This is the transcribed text.'}

        # Set up a mock response from openai.Completion.create for GPT summary
        mock_openai_create.return_value = {
            'choices': [{'text': 'This is a mocked summary of the transcription.'}]
        }

        # Set up a mock response for receiving messages from the queue
        mock_receive_messages.return_value = [
            ServiceBusMessage(body="mock_audio_data", message_id="5678")
        ]

        manager = Manager()
        log_queue = manager.Queue()

        # Prepare configuration and log queue (mock or use empty dicts for simplicity in tests)
        config = ProcessingWorkerConfiguration(
            input_queue_name="input-queue",
            output_queue_name="output-queue",
            service_bus_connection_str="dummy_connection_string",
            stats_dict={}  # Empty stats dict for simplicity
        )

        # Initialize the SpeechToTextProcessingWorker
        worker = SpeechToTextProcessingWorker(worker_id=1, config=config, log_queue=log_queue, stats_dict={})

        # Run the worker's main method (this will simulate receiving, processing, and sending messages)
        worker.run()

        # Assert that a message was received from the input queue
        mock_receive_messages.assert_called_once()

        # Assert that the Whisper model was called to transcribe the audio data
        mock_transcribe.assert_called_once_with("mock_audio_data")

        # Assert that the GPT API was called to summarize the transcription
        mock_openai_create.assert_called_once_with(
            model="gpt-4",
            prompt="Summarize the following text:\n\nThis is the transcribed text.",
            max_tokens=150
        )

        # Assert that the completion was called to complete the message in the queue
        mock_complete_message.assert_called_once()

        # Assert that a message was sent to the output queue
        mock_send_messages.assert_called_once()


if __name__ == '__main__':
    unittest.main()
