import os
import threading
import json
import queue
import copy

from rev_ai.streamingclient import RevAiStreamingClient
from rev_ai.models import MediaConfig
from google.cloud import speech

from typing import List, Any, Dict

import Utils as Utils
from CommonTypes import EosFragmentEncodings
from Languages import EosLanguage
from GoogleCloudApi import GoogleCloudStreamingGenerator, GoogleCloudApiListener

STREAMING_LIMIT = 240000  # 4 minutes
# STREAMING_LIMIT = 120000  # 2 minutes
DEBUG = False


####################################################
#
#  RevAitreamingTranscribe
#
####################################################
class RevAitreamingTranscribe(threading.Thread):
    _access_token: str
    _src_language: EosLanguage
    _generator: GoogleCloudStreamingGenerator
    _client: RevAiStreamingClient
    _listener: GoogleCloudApiListener
    _streaming_config: MediaConfig
    _ready: threading.Event

    ####################################################
    #  __init__
    ####################################################
    def __init__(self,
                 ready: threading.Event,
                 src_language: EosLanguage,
                 sample_rate: int,
                 generator: GoogleCloudStreamingGenerator,
                 listener: GoogleCloudApiListener):

        self._access_token = 'your_access_token'

        self._src_language = src_language
        self._generator = generator
        self._listener = listener

        self._streaming_config = MediaConfig(content_type='audio/x-raw',
                                             layout='interleaved',
                                             rate=16000,  # sample_rate,
                                             audio_format='S16LE',
                                             channels=1)

        self._client = RevAiStreamingClient(self._access_token,
                                            self._streaming_config,
                                            on_error=RevAitreamingTranscribe.on_error,
                                            on_close=RevAitreamingTranscribe.on_close,
                                            on_connected=RevAitreamingTranscribe.on_connected)

        self._ready = ready

        threading.Thread.__init__(self, name='RevAitreamingTranscribe')  # start thread

    ####################################################
    #  on_error, on_close, on_connected
    ####################################################
    @staticmethod
    def on_error(error: str) -> None:
        print('RevAitreamingTranscribe::on_error ************************************************* {}'.format(error))

    @staticmethod
    def on_close(code: str, reason: str) -> None:
        print('RevAitreamingTranscribe::on_close ************************************************* {}, {}'.format(code, reason))

    @staticmethod
    def on_connected(id: str) -> None:
        print('RevAitreamingTranscribe::on_connected ************************************************* {}'.format(id))

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('RevAitreamingTranscribe', "RevAitreamingTranscribe::run thread started name={}".format(self.getName()))

        self._ready.set()

        with self._generator as stream:

            # while not stream.closed:

            # Starts the server connection and thread sending microphone audio
            response_gen = self._client.start(stream.generator(),
                                              metadata=None,
                                              custom_vocabulary_id=None,
                                              filter_profanity=None,
                                              remove_disfluencies=None,
                                              delete_after_seconds=None)

            # Iterates through responses and prints them
            for response in response_gen:
                #print('======================================================================')
                #print(response)
                #print('======================================================================')
                self._handle_response(response, stream)

            Utils.logger_.debug_color('RevAitreamingTranscribe', "RevAitreamingTranscribe::run {}: NEW REQUEST".format(stream.start_time))

                # stream.new_stream = True

        # notify listener that transcribing is over
        dummy_words = []
        self._listener.handle_words(dummy_words, 0)

        Utils.logger_.system('RevAitreamingTranscribe', "RevAitreamingTranscribe::run thread ending name={}".format(self.getName()))

    ####################################################
    # _handle_responses
    ####################################################
    def _handle_response(self, response, stream):

        response_json_ = json.loads(response)

        response_type = response_json_['type']
        if(response_type == 'final'):

            print(response_json_)

            elements = response_json_['elements']

            words = []
            transcript = ''
            for element in elements:
                transcript += element['value']

                if element['type'] == 'text':
                    word = speech.types.WordInfo()  # TODO: replace with InnerWord
                    word.word = element['value']
                    start_time = element['ts']
                    end_time = element['end_ts']
                    word.start_time.seconds = int(start_time)
                    word.start_time.microseconds = int((start_time - word.start_time.seconds) * 1000000.0)
                    word.end_time.seconds = int(end_time)
                    word.end_time.microseconds = int((end_time - word.end_time.seconds) * 1000000.0)

                    words.append(word)

                elif element['type'] == 'punct':
                    if element['value'] != ' ':
                        if len(words) > 0:
                            word = words[-1]
                            word.word += element['value']


            transcript_to_use = transcript
            if self._src_language.right_to_left() is True:
                transcript_to_use = transcript[::-1]

            if len(words) > 0:
                Utils.logger_.debug_color('RevAitreamingTranscribe', "RevAitreamingTranscribe::_handle_response {}".format(transcript_to_use))
                print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                print("words={}".format(words))
                print("stream.start_time / 1000.0={}".format(stream.start_time / 1000.0))
                print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                self._listener.handle_words(words, stream.start_time / 1000.0)

