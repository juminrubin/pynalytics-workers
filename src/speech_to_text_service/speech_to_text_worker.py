# src/speech_to_text_service/speech_to_text_service.py

import hashlib
from typing import Dict, Any

from shared.abstract_processing import AbstractProcessingWorker, AbstractProcessRequest, DefaultProcessingResponse, \
    ProcessingStatus
from shared.cache import get_cache

class SpeechToTextProcessRequest(AbstractProcessRequest):
    def get_data(self) -> str:
        return self.request_input  # Assuming request_input is the audio file path or bytes.

class SpeechToTextProcessResponse(DefaultProcessingResponse):
    def __init__(self, request_id: str, audio_length: int, status: ProcessingStatus, processing_result: Any = None, error_message: str = None) -> None:
        super().__init__(request_id, status, processing_result, error_message)
        self.audio_length = audio_length

    def to_dict(self) -> Dict[str, Any]:
        return {"request_id": self.request_id, "status": self.status, "processing_result": self.processing_result, "audio_length": self.audio_length}

class SpeechToTextProcessingWorker(AbstractProcessingWorker):
    def __init__(self, worker_id: int, config, log_queue, stats_dict: Dict[int, Dict[str, int]]) -> None:
        super().__init__(worker_id, config, log_queue, stats_dict)
        self.cache = get_cache()
        #self.model = whisper.load_model("base")

    def create_request_from_message(self, message) -> AbstractProcessRequest:
        return SpeechToTextProcessRequest(request_id=message.message_id, request_input=message.body)

    async def process_message(self, request: AbstractProcessRequest) -> DefaultProcessingResponse:
        audio_data = request.get_data()
        audio_length = len(audio_data)

        cache_key = self._get_cache_key(audio_data)
        cached_result = self.cache.get(cache_key)
        if cached_result:
            self.logger.info(f"Cache hit for request {request.request_id}")
            return SpeechToTextProcessResponse(request_id=request.request_id, audio_length=audio_length, status=ProcessingStatus.SUCCESS, processing_result=cached_result)

        try:
            #result = self.model.transcribe(audio_data)
            #transcription = result['text']
            transcription = "dummy transcript"
            self.cache.set(cache_key, transcription)
            return SpeechToTextProcessResponse(request_id=request.request_id, audio_length=audio_length, status=ProcessingStatus.SUCCESS, processing_result=transcription)
        except Exception as e:
            self.logger.error(f"Error processing request {request.request_id}: {str(e)}")
            return SpeechToTextProcessResponse(request_id=request.request_id, audio_length=audio_length, status=ProcessingStatus.ERROR, error_message=str(e))

    def _get_cache_key(self, audio_data: str) -> str:
        return hashlib.sha256(audio_data.encode('utf-8')).hexdigest()
