# src/shared/abstract_processing.py

from abc import ABC, abstractmethod
import asyncio
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver
from azure.servicebus.exceptions import ServiceBusError
from enum import Enum
import json
import logging
from multiprocessing.queues import Queue

from shared.logging_config import log_worker_configurer

import time
from typing import Any, Dict

ATTR_SUCCESS_COUNT = "success_count"
ATTR_ERROR_COUNT = "error_count"

class ProcessingStatus(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"

class AbstractProcessRequest(ABC):
    def __init__(self, request_id: str, request_input: Any) -> None:
        self.request_id: str = request_id
        self.request_input: Any = request_input

    @abstractmethod
    def get_data(self) -> Any:
        pass


class DefaultProcessingResponse(ABC):
    def __init__(self, request_id: str, status: ProcessingStatus, processing_result: Any = None, error_message: str = None) -> None:
        self.request_id: str = request_id
        self.status: ProcessingStatus = status
        self.processing_result: Any = processing_result
        self.error_message = error_message

    def to_dict(self) -> Dict[str, Any]:
        return {"request_id": self.request_id, "status": self.status, "processing_result": self.processing_result, "error_message": self.error_message}


class ProcessingWorkerConfiguration:
    def __init__(self, input_queue_name: str, output_queue_name: str, service_bus_connection_str: str) -> None:
        self.input_queue_name: str = input_queue_name
        self.output_queue_name: str = output_queue_name
        self.service_bus_connection_str: str = service_bus_connection_str


class AbstractProcessingWorker(ABC):
    def __init__(self, worker_id: int, config: ProcessingWorkerConfiguration, log_queue: Queue, stats_dict: Dict[int, Dict[str, int]], timeout_seconds: int = 300) -> None:
        self.worker_id: int = worker_id
        self.config: ProcessingWorkerConfiguration = config
        self.stats_dict: Dict[int, Dict[str, int]] = stats_dict
        self.idle_sleep_time: int = 3
        self.timeout_seconds = timeout_seconds

        self.log_queue = log_queue
        log_worker_configurer(self.log_queue)  # Configure the logging for this worker

        self.logger = logging.getLogger(f'Worker-{self.worker_id}')
        self.logger.info(f'Worker {self.worker_id} initialized')

    def run(self) -> None:
        service_bus_client: ServiceBusClient = ServiceBusClient.from_connection_string(
            self.config.service_bus_connection_str)
        input_queue_receiver: ServiceBusReceiver = service_bus_client.get_queue_receiver(
            queue_name=self.config.input_queue_name)
        output_queue_sender = service_bus_client.get_queue_sender(queue_name=self.config.output_queue_name)

        while True:
            try:
                with input_queue_receiver:
                    received_msgs = input_queue_receiver.receive_messages(max_message_count=1, max_wait_time=5)
                    if not received_msgs:
                        self.handle_idle_state()
                        continue

                    # Process messages if available
                    for msg in received_msgs:
                        input_queue_receiver.complete_message(msg)

                        request: AbstractProcessRequest = self.create_request_from_message(msg)
                        response: DefaultProcessingResponse = asyncio.run(self.process_message_with_timeout(request))

                        output_queue_sender.send_messages(ServiceBusMessage(json.dumps(response.to_dict())))

                        self.update_statistics_journal(response.status == ProcessingStatus.SUCCESS)

                        self.reset_idle_sleep()

            except ServiceBusError as e:
                self.logger.error(f"ServiceBus error occurred: {str(e)}")
            except Exception as e:
                self.logger.error(f"An error occurred in worker: {str(e)}")

    def create_processing_timeout_response(self, request_id: str, error_message: str) -> DefaultProcessingResponse:
        return DefaultProcessingResponse(request_id=request_id, status=ProcessingStatus.ERROR, error_message=error_message)

    @abstractmethod
    def create_request_from_message(self, message) -> AbstractProcessRequest:
        pass

    @abstractmethod
    async def process_message(self, request: AbstractProcessRequest) -> DefaultProcessingResponse:
        pass

    async def process_message_with_timeout(self, request: AbstractProcessRequest) -> DefaultProcessingResponse:
        """
        Wrapper to call process_message with a timeout of 5 minutes.
        """
        try:
            # Use asyncio.wait_for to enforce the timeout
            return await asyncio.wait_for(self.process_message(request), timeout=self.timeout_seconds)
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout occurred for request ID {request.request_id}")
            return self.create_processing_timeout_response(request_id=request.request_id, error_message=f"Processing timeout after {self.timeout_seconds} seconds. Please retry again.")

    def update_statistics_journal(self, success: bool) -> None:
        """
        Update success or failure count in the worker-specific stats.
        """
        if success:
            self.stats_dict[self.worker_id][ATTR_SUCCESS_COUNT] += 1
        else:
            self.stats_dict[self.worker_id][ATTR_ERROR_COUNT] += 1

    def handle_idle_state(self) -> None:
        """
        If no message is available, the worker sleeps for a while.
        Sleep time increases up to a maximum of 10 seconds.
        """
        print(f"No messages. Worker {self.worker_id} is sleeping for {self.idle_sleep_time} seconds.")
        time.sleep(self.idle_sleep_time)
        self.idle_sleep_time = min(self.idle_sleep_time + 1, 10)  # Increase sleep time, max 10 seconds

    def reset_idle_sleep(self) -> None:
        """
        Resets the idle sleep time back to the default.
        """
        self.idle_sleep_time = 3