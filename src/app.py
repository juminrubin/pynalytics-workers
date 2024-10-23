# app.py
from multiprocessing.queues import Queue
from typing import Dict

from fastapi import FastAPI
from multiprocessing import Manager, Process
import os
from shared.logging_config import start_log_listener, stop_log_listener
from shared.abstract_processing import ProcessingWorkerConfiguration, AbstractProcessingWorker, ATTR_SUCCESS_COUNT, ATTR_ERROR_COUNT
from summarization_service.summarization_worker import SummarizationProcessingWorker
from speech_to_text_service.speech_to_text_worker import SpeechToTextProcessingWorker


SERVICE_BUS_CONNECTION_STR: str = os.getenv('SERVICE_BUS_CONNECTION_STR', '')
INPUT_QUEUE_NAME: str = os.getenv('INPUT_QUEUE_NAME', '')
OUTPUT_QUEUE_NAME: str = os.getenv('OUTPUT_QUEUE_NAME', '')
APP_WORKER_SIZE: int = int(os.getenv('APP_WORKER_SIZE', 2))
PROCESSING_TIMEOUT: int = 600  # 10 minutes

app = FastAPI()

def start_workers(worker_class: AbstractProcessingWorker, config: ProcessingWorkerConfiguration, log_queue: Queue, stats_dict: Dict[int, Dict[str, int]], worker_count: int = 1, timeout_seconds: int = 60):
    processes = []
    for i in range(worker_count):
        worker = worker_class(i, config, log_queue, stats_dict)
        p = Process(target=worker.run)
        p.start()
        processes.append(p)
    return processes

@app.get("/stats")
async def get_stats():
    total_success = sum(manager_stats[worker][ATTR_SUCCESS_COUNT] for worker in manager_stats)
    total_failure = sum(manager_stats[worker][ATTR_ERROR_COUNT] for worker in manager_stats)
    return {"success": total_success, "failure": total_failure}

if __name__ == "__main__":
    manager = Manager()
    manager_stats = manager.dict()
    log_queue = manager.Queue()

    config = ProcessingWorkerConfiguration(
        input_queue_name=INPUT_QUEUE_NAME,
        output_queue_name=OUTPUT_QUEUE_NAME,
        service_bus_connection_str=SERVICE_BUS_CONNECTION_STR
    )

    log_listener = start_log_listener(log_queue)

    try:
        processes = start_workers(SummarizationProcessingWorker, config, log_queue, manager_stats, 2, 60)
        processes += start_workers(SpeechToTextProcessingWorker, config, log_queue, manager_stats, 2, 300)

        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)

        for p in processes:
            p.join()
    finally:
        stop_log_listener(log_queue, log_listener)
