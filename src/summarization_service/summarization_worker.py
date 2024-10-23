# src/summarization_service/summarization_service.py

import hashlib
import openai
from shared.abstract_processing import AbstractProcessingWorker, AbstractProcessRequest, DefaultProcessingResponse, \
    ProcessingStatus
from shared.cache import get_cache
from typing import Any, Dict

class SummarizationProcessRequest(AbstractProcessRequest):
    def get_data(self) -> str:
        return self.request_input

class SummarizationProcessingWorker(AbstractProcessingWorker):

    def __init__(self, worker_id: int, config, log_queue, stats_dict: Dict[int, Dict[str, int]]) -> None:
        super().__init__(worker_id, config, log_queue, stats_dict)
        self.cache = get_cache()

    def create_request_from_message(self, message) -> AbstractProcessRequest:
        return SummarizationProcessRequest(request_id=message.message_id, request_input=message.body)

    async def process_message(self, request: AbstractProcessRequest) -> DefaultProcessingResponse:
        input_text:str = request.get_data()
        cache_key = self._get_cache_key(input_text)
        cached_result = self.cache.get(cache_key)
        if cached_result:
            self.logger.info(f"Cache hit for request {request.request_id}")
            return DefaultProcessingResponse(request_id=request.request_id, status=ProcessingStatus.SUCCESS, processing_result=cached_result)
        try:
            #response = await openai.Completion.create(model="gpt-4", prompt=f"Summarize the following text:\n\n{input_text}", max_tokens=150)
            #summary = response['choices'][0]['text'].strip()
            summary = f"Summary the following text: {input_text[:100]}"
            self.cache.set(cache_key, summary)
            return DefaultProcessingResponse(request_id=request.request_id, status=ProcessingStatus.SUCCESS, processing_result=summary)
        except Exception as e:
            self.logger.error(f"Error processing request {request.request_id}: {str(e)}")
            return DefaultProcessingResponse(request_id=request.request_id, status=ProcessingStatus.ERROR, error_message=str(e))

    def _get_cache_key(self, input_text: str) -> str:
        return hashlib.sha256(input_text.encode('utf-8')).hexdigest()

