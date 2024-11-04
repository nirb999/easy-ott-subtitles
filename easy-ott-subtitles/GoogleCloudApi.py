import os
import threading
import queue
import copy

from google.cloud import translate
from google.cloud import speech
from google.api_core import exceptions

from typing import List, Any, Dict

import Utils as Utils
from CommonTypes import EosFragmentEncodings
from Languages import EosLanguage

STREAMING_LIMIT = 180000  # 3 minutes
# STREAMING_LIMIT = 120000  # 2 minutes
DEBUG = False

GOOGLE_API__PROJECT_ID = Utils.ConfigVariable('GOOGLE_API', 'PROJECT_ID', type=str, default_value='', description='Google Cloud project ID', mandatory=False)
GOOGLE_API__SERVICE_ACCOUNT_FILE = Utils.ConfigVariable('GOOGLE_API', 'SERVICE_ACCOUNT_FILE', type=str, default_value='', description='Path to Google service acount json file', mandatory=False)


####################################################
#
#  GoogleCloudApi
#
####################################################
class GoogleCloudApi:

    __project_id: str
    __service_account_file: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__project_id = GOOGLE_API__PROJECT_ID.value()
        self.__service_account_file = GOOGLE_API__SERVICE_ACCOUNT_FILE.value()

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.__service_account_file

    ####################################################
    #  translate
    ####################################################
    def translate(self, text: List[str], src_language: EosLanguage, dst_language: EosLanguage) -> List:

        try:
            client = translate.TranslationServiceClient()

            parent = "projects/" + self.__project_id + "/locations/global"

            response = client.translate_text(request={"parent": parent,
                                                      "contents": text,
                                                      "mime_type": "text/plain",
                                                      "source_language_code": src_language.code_639_1(),
                                                      "target_language_code": dst_language.code_639_1()})

        except exceptions.GoogleAPIError as err:
            error_str = "GoogleCloudApi::translate exception {}".format(err)
            Utils.logger_.error('GoogleCloudApi', error_str)
            return []

        return response.translations


####################################################
#
#  GoogleCloudApiListener
#
####################################################
class GoogleCloudApiListener:

    __prev_end_time: int
    __sentence: List[Any]
    __src_language: EosLanguage
    __dst_languages: List[EosLanguage]
    __google_api: GoogleCloudApi
    __current_text: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, src_language: EosLanguage, dst_languages: List[EosLanguage]):

        self.__prev_end_time = 0
        self.__sentence = []

        self.__src_language = src_language
        self.__dst_languages = dst_languages

        self.__google_api = GoogleCloudApi()

        self.__current_text = ''

    #################################
    # InnerWordTime
    #################################
    class InnerWordTime:
        seconds: int
        microseconds: int

        def __init__(self):
            self.seconds = 0
            self.microseconds = 0

    #################################
    # InnerWord
    #################################
    class InnerWord:
        word: str
        # start_time: InnerWordTime
        # end_time: InnerWordTime

        def __init__(self):
            self.word = ''
            self.start_time = None
            self.end_time = None

    #################################
    # handle_words
    #################################
    def handle_words(self, words: List, time_offset: float) -> None:
        pass

    #################################
    # _words_to_sentences
    #################################
    def _words_to_sentences(self, words_: Dict[str, Any]) -> None:

        words = words_['words']
        time_offset: float = words_['time_offset']

        # flush self.__sentence
        if len(words) == 0:
            #print("GoogleCloudApiListener::_words_to_sentences flush last sentence")
            if len(self.__sentence) > 0:
                self._finalize_text(self.__sentence)
                self.__sentence = []
            return

        break_found = False

        for _word in words:

            word = GoogleCloudApiListener.InnerWord()
            word.word = _word.word
            word.start_time = GoogleCloudApiListener.InnerWordTime()
            if _word.start_time.seconds:
                word.start_time.seconds = _word.start_time.seconds
            if _word.start_time.microseconds:
                word.start_time.microseconds = _word.start_time.microseconds
            word.end_time = GoogleCloudApiListener.InnerWordTime()
            if _word.end_time.seconds:
                word.end_time.seconds = _word.end_time.seconds
            if _word.end_time.microseconds:
                word.end_time.microseconds = _word.end_time.microseconds

            self._add_time(word.start_time, time_offset)
            self._add_time(word.end_time, time_offset)

            self.__sentence.append(word)

            # if sentence ends with punctuation and is long enough
            if word.word.endswith('.') or word.word.endswith(',') or word.word.endswith(':') or word.word.endswith('?') or word.word.endswith('!') or word.word.endswith(';'):
                if len(self.__sentence) > 1:
                    if (self._calculate_time(self.__sentence[-1].end_time) - self._calculate_time(self.__sentence[0].start_time)) > 1:
                        self._finalize_text(self.__sentence)
                        self.__sentence = []
                        break_found = True

            #word_duration = round(self._calculate_time(word.end_time) - self._calculate_time(word.start_time), 2)
            word_diff_to_prev = round(self._calculate_time(word.start_time) - self.__prev_end_time, 2)
            #word_ratio = round((self._calculate_time(word.end_time) - self._calculate_time(word.start_time)) / len(word.word), 2)
            #print("word=({}), duration={}, diff to prev={}, ratio={}".format(word.word, word_duration, word_diff_to_prev, word_ratio))

            # if we have a long word_diff_to_prev
            if len(self.__sentence) > 1:
                # if word_duration + word_diff_to_prev >= 0.5 and word_ratio >= 0.2:
                if word_diff_to_prev > 0.7:
                    last_word = self.__sentence[len(self.__sentence) - 1]
                    self._finalize_text(self.__sentence[0:-1])
                    self.__sentence = []
                    self.__sentence.append(last_word)
                    break_found = True

            self.__prev_end_time = self._calculate_time(word.end_time)

        if break_found is False:
            if len(self.__sentence) > 0:
                if (self._calculate_time(self.__sentence[-1].end_time) - self._calculate_time(self.__sentence[0].start_time)) > 1:
                    self._finalize_text(self.__sentence)
                    self.__sentence = []

    #################################
    # _finalize_text
    #################################
    def _finalize_text(self, sentence: List[EosLanguage]) -> None:

        # translate if needed
        self.__current_text = ''
        for dst_lang in self.__dst_languages:
            if dst_lang != self.__src_language:
                self._handle_translation(self.__src_language, dst_lang, sentence)

        self._break_sentence(self.__src_language, sentence)

    #################################
    # _break_sentence
    #################################
    def _break_sentence(self, language: EosLanguage, sentence: List[Any]) -> None:

        max_chars = 35  # 42
        max_lines = 2

        # break text to sentences

        num_chars = 0
        for word in sentence:
            num_chars += len(word.word)
            num_chars += 1  # white space
        num_chars -= 1  # last white space

        word_index = 0
        completed_sentences = []
        while word_index < len(sentence):
            sentences_to_write = []
            line_index = 0
            while line_index < max_lines:
                current_sentence = []
                written_chars = 0
                while written_chars + len(sentence[word_index].word) <= max_chars:
                    current_sentence.append(sentence[word_index])
                    written_chars += len(sentence[word_index].word)
                    word_index += 1

                    if word_index == len(sentence):
                        break

                sentences_to_write.append(current_sentence)

                line_index += 1

                if word_index == len(sentence):
                    break

            completed_sentences.append(sentences_to_write)

        # now balance sentences
        if len(completed_sentences) > 1:
            if(len(completed_sentences[-1]) == 1):
                if(len(completed_sentences[-1][-1]) <= 2):
                    if completed_sentences[-2] == 1 or len(completed_sentences[-1][-1]) == 1:
                        for index in range(len(completed_sentences[-1][-1])):
                            completed_sentences[-2][-1].append(completed_sentences[-1][-1][index])
                    else:
                        completed_sentences[-2][-2].append(completed_sentences[-2][-1][0])
                        del completed_sentences[-2][-1][0]
                        for index in range(len(completed_sentences[-1][-1])):
                            completed_sentences[-2][-1].append(completed_sentences[-1][-1][index])
                    del completed_sentences[-1]

        for s in completed_sentences:
            sentences_to_handle = []
            for current_sentence in s:

                # completed line
                line = ''
                first = True
                for word in current_sentence:
                    if first is True:
                        first = False
                    else:
                        line += ' '
                    line += word.word
                sentences_to_handle.append({'start': round(self._calculate_time(current_sentence[0].start_time), 2),
                                            'end': round(self._calculate_time(current_sentence[-1].end_time), 2),
                                            'text': line})

            # print("sentences_to_handle: ", sentences_to_handle)
            self._handle_text(language, sentences_to_handle)

    #################################
    # _handle_translation
    #################################
    def _handle_translation(self, src_language: EosLanguage, dst_language: EosLanguage, sentence: List[Any]) -> None:

        if self.__current_text == '':
            first = True
            for word in sentence:
                if first is True:
                    first = False
                else:
                    self.__current_text += ' '
                self.__current_text += word.word

        #print("self.__current_text: ", self.__current_text)
        translations = self.__google_api.translate([self.__current_text], self.__src_language, dst_language)
        #for translation in translations:
        #    print("translation: ", translation.translated_text[::-1])

        if len(translations) > 0:

            words = translations[0].translated_text.split(' ')

            num_chars = 0
            for word in words:
                num_chars += len(word)

            total_time = self._calculate_time(sentence[-1].end_time) - self._calculate_time(sentence[0].start_time)
            char_time = total_time / float(num_chars)

            translated_sentence = []

            current_time = self._calculate_time(sentence[0].start_time)
            for word in words:
                new_word = GoogleCloudApiListener.InnerWord()
                new_word.word = word
                new_word.start_time = self._time_to_word_time(current_time)
                current_time += len(word) * char_time
                new_word.end_time = self._time_to_word_time(current_time)
                translated_sentence.append(new_word)

            self._break_sentence(dst_language, translated_sentence)

    #################################
    # _handle_text
    #################################
    def _handle_text(self, language: EosLanguage, lines: List[Dict[str, Any]]) -> None:
        pass

    #################################
    # _calculate_time
    #################################
    def _calculate_time(self, time) -> float:
        time_ = float(float(time.seconds) + float(time.microseconds * 1e-6))
        return time_

    #################################
    # _time_to_word_time
    #################################
    def _time_to_word_time(self, time: float):  # -> GoogleCloudApiListener.InnerWordTime:

        word_time = GoogleCloudApiListener.InnerWordTime()
        word_time.seconds = int(time)
        word_time.microseconds = int((time - word_time.seconds) * 1e6)
        return word_time

    #################################
    # _add_time
    #################################
    def _add_time(self, word_time, time_offset: float) -> None:

        time_offset_seconds = int(time_offset)
        time_offset_microseconds = (time_offset - time_offset_seconds) * 1e6

        word_time_microseconds = time_offset_microseconds + word_time.microseconds
        word_time_seconds = time_offset_seconds + word_time.seconds

        if word_time_microseconds >= 1e6:
            word_time_seconds += 1
            word_time_microseconds -= 1e6

        word_time.seconds = int(word_time_seconds)
        word_time.microseconds = int(word_time_microseconds)


####################################################
#
#  GoogleCloudStreamingGenerator
#
####################################################
class GoogleCloudStreamingGenerator:

    current_time: float
    start_time: float
    final_result_end_time: float

    #################################
    #  __init__
    #################################
    def __init__(self, sample_rate: int):
        self.sample_rate = sample_rate
        self._queue = queue.Queue()
        self.closed = True
        self.current_time = 0.0
        self.start_time = self.current_time
        self.last_audio_input = b''
        self.final_result_end_time = 0.0
        self.new_stream = True
        self.last_chunk = False

        self.__total_time_sent_to_stt_engine = float(0)
        self.__total_time_read_from_source = float(0)

        self.debug_in_file = "in.pcm"
        self.debug_out_file = "out"
        self.debug_out_file_index = 0
        self.out_bytes = 0

    #################################
    #  close
    #################################
    def close(self) -> None:

        Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::close")

        self.closed = True

    #################################
    #  __enter__
    #################################
    def __enter__(self):
        Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::__enter__")

        self.closed = False
        return self

    #################################
    #  __exit__
    #################################
    def __exit__(self, type, value, traceback):
        Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::__exit__")

        self.closed = True

        Utils.logger_.info('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::__exit__ __total_time_sent_to_stt_engine={}, __total_time_read_from_source={}".format(self.__total_time_sent_to_stt_engine, self.__total_time_read_from_source))

    #################################
    # put_fragment
    #################################
    def put_fragment(self, audio_data):
        self._queue.put(audio_data)

    #################################
    # get_engine_time
    #################################
    def get_engine_time(self) -> float:
        return self.__total_time_sent_to_stt_engine

    #################################
    # generator
    #################################
    def generator(self):
        Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::generator called")

        while not self.closed:

            if self.last_chunk is True:
                self.closed = True
                return

            data = []

            if self.new_stream:

                #print("--------------------------------------------------------- NEW GENERATOR STREAM")

                Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::generator new stream")
                Utils.logger_.info('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::generator __total_time_sent_to_stt_engine={}, __total_time_read_from_source={}".format(self.__total_time_sent_to_stt_engine, self.__total_time_read_from_source))

                if len(self.last_audio_input) > 0:

                    data.append(copy.deepcopy(self.last_audio_input))  # TODO: do we need the deepcopy

                    Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::generator new stream copied {} bytes".format(len(self.last_audio_input)))
                    self.__total_time_sent_to_stt_engine += round(float(len(self.last_audio_input)) / float(self.sample_rate * 2), 2)

                self.new_stream = False

            chunk = self._queue.get()

            if chunk is None:
                Utils.logger_.debug('GoogleCloudStreamingGenerator', "GoogleCloudStreamingGenerator::generator got null chunk")
                # handle leftover in self.last_audio_input and then close
                self.last_chunk = True

                if(len(data) == 0):
                    return

            else:

                data.append(chunk)
                self.last_audio_input += chunk

                # ## debug ##
                if DEBUG is True:
                    with open(self.debug_in_file, 'ab') as _debug_in_file:
                        _debug_in_file.write(chunk)
                # ## debug ##

                self.current_time += round(float(len(chunk)) / float(self.sample_rate * 2) * 1000, 2)

                self.__total_time_sent_to_stt_engine += round(float(len(chunk)) / float(self.sample_rate * 2), 2)
                #print("self.__total_time_sent_to_stt_engine 2={}".format(self.__total_time_sent_to_stt_engine))
                self.__total_time_read_from_source += round(float(len(chunk)) / float(self.sample_rate * 2), 2)

                if self.last_chunk is False:
                    while True:
                        try:
                            chunk = self._queue.get(block=False)

                            if chunk is None:
                                return
                            data.append(chunk)
                            self.last_audio_input += chunk

                            self.current_time += round(float(len(chunk)) / float(self.sample_rate * 2) * 1000, 2)

                            self.__total_time_sent_to_stt_engine += round(float(len(chunk)) / float(self.sample_rate * 2), 2)
                            #print("self.__total_time_sent_to_stt_engine 2={}".format(self.__total_time_sent_to_stt_engine))
                            self.__total_time_read_from_source += round(float(len(chunk)) / float(self.sample_rate * 2), 2)

                        except queue.Empty:
                            break

            yield b''.join(data)


####################################################
#
#  GoogleCloudStreamingTranscribe
#
####################################################
class GoogleCloudStreamingTranscribe(threading.Thread):
    _src_language: EosLanguage
    _generator: GoogleCloudStreamingGenerator
    _client: speech.SpeechClient
    _listener: GoogleCloudApiListener
    _streaming_config: speech.StreamingRecognitionConfig
    _ready: threading.Event

    _engine_accuracy_total: float
    _engine_accuracy_count: int

    ####################################################
    #  __init__
    ####################################################
    def __init__(self,
                 ready: threading.Event,
                 src_language: EosLanguage,
                 sample_rate: int,
                 generator: GoogleCloudStreamingGenerator,
                 listener: GoogleCloudApiListener):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_API__SERVICE_ACCOUNT_FILE.value()

        self._src_language = src_language
        self._generator = generator
        self._listener = listener

        self._client = speech.SpeechClient()

        metadata = speech.RecognitionMetadata(interaction_type=speech.RecognitionMetadata.InteractionType.INTERACTION_TYPE_UNSPECIFIED,
                                              original_media_type=speech.RecognitionMetadata.OriginalMediaType.VIDEO,
                                              recording_device_type=speech.RecognitionMetadata.RecordingDeviceType.RECORDING_DEVICE_TYPE_UNSPECIFIED)

        diarization_config = speech.SpeakerDiarizationConfig(enable_speaker_diarization=False,
                                                             min_speaker_count=2,
                                                             max_speaker_count=6)

#        phrases = ['ari taub', 'ari', 'taub', 'john nguyen', 'nguyen', 'Justin Basra', 'Justim', 'Basra', 'Nick Ring', 'Cory Devela', 'Devela']
#        speech_contexts = speech.SpeechContext(phrases=phrases)

        config = speech.RecognitionConfig(encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
                                          sample_rate_hertz=sample_rate,
                                          language_code=self._src_language.code_bcp_47(),
                                          #metadata=metadata,
                                          #diarization_config=diarization_config,
                                          enable_word_time_offsets=True,
                                          # speech_contexts=[speech_contexts],
                                          enable_automatic_punctuation=True,
                                          max_alternatives=1,
                                          model=self._src_language.model(),
                                          use_enhanced=self._src_language.enhanced())

        self._streaming_config = speech.StreamingRecognitionConfig(config=config,
                                                                   # single_utterance=True,
                                                                   interim_results=True)

        self._ready = ready

        self._engine_accuracy_total = float(0)
        self._engine_accuracy_count = 0

        threading.Thread.__init__(self, name='GoogleCloudStreamingTranscribe')  # start thread

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::run thread started name={}".format(self.getName()))

        self._ready.set()

        with self._generator as stream:

            while not stream.closed:

                try:
                    Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::run {}: NEW REQUEST".format(stream.start_time))

                    audio_generator = stream.generator()

                    requests = (speech.StreamingRecognizeRequest(audio_content=content)for content in audio_generator)

                    responses = self._client.streaming_recognize(self._streaming_config, requests)

                    Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', '1')

                    # Now, put the transcription responses to use.
                    self._handle_responses(responses, stream)

                    Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', '2')

                    stream.new_stream = True

                except exceptions.GoogleAPIError as err:
                    error_str = "GoogleCloudStreamingTranscribe::run exception timeout {}".format(err)
                    Utils.logger_.error('GoogleCloudStreamingTranscribe', error_str)

        # notify listener that transcribing is over
        dummy_words = []
        self._listener.handle_words(dummy_words, 0)

        self._ready.set()

        Utils.logger_.system('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::run thread ending name={}".format(self.getName()))

    ####################################################
    # _handle_responses
    ####################################################
    def _handle_responses(self, responses, stream):

        for response in responses:

            if not response.results:
                continue

            result = response.results[0]

            if not result.alternatives:
                continue

            if result.is_final:

                transcript = result.alternatives[0].transcript
                confidence = result.alternatives[0].confidence

                self._engine_accuracy_total += (confidence * 100.0)
                self._engine_accuracy_count += 1

                transcript_to_use = transcript
                if self._src_language.right_to_left() is True:
                    transcript_to_use = transcript[::-1]

                if len(result.alternatives[0].words) > 0:
                    first_word_start_time = (self._calculate_time(result.alternatives[0].words[0].start_time) + stream.start_time) / 1000
                    Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses {}: {} {}".format(first_word_start_time, confidence, transcript_to_use))
                    #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                    #print("result.alternatives[0].words={}".format(result.alternatives[0].words))
                    #_start_time = stream.start_time / 1000.0
                    #for word in result.alternatives[0].words:
                    #    print("{},        start={}, end={}".format(word.word, self._add_time(word.start_time, _start_time), self._add_time(word.end_time, _start_time)))
                    #print("stream.start_time / 1000.0={}".format(stream.start_time / 1000.0))
                    #print("transcript_to_use: ", transcript_to_use)
                    #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
                    self._listener.handle_words(result.alternatives[0].words, stream.start_time / 1000.0)

                # calcuate the result end time as the middle between result.result_end_time and the end_time of the last word in the result
                result_end_time = self._calculate_time(result.result_end_time)
                #print("result_end_time={}".format(result_end_time))
                end_time = result_end_time
                #if len(result.alternatives[0].words) > 0:
                #    end_time = self._calculate_time(result.alternatives[0].words[-1].end_time)
                #    print("end_time={}".format(end_time))
                Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses result_end_time={}, end_time={}".format(result_end_time, end_time))

                # calculate how many bytes we need to remove from stream.last_audio_input
                new_final_result_end_time = end_time + float(stream.start_time)
                #print("new_final_result_end_time={}".format(new_final_result_end_time))
                time_diff = new_final_result_end_time - stream.final_result_end_time
                offset = round(time_diff * stream.sample_rate * 2 / 1000)

                Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses new_final_result_end_time={}, stream.final_result_end_time={}, time_diff={}, offset={}, len(stream.last_audio_input)={}".format(new_final_result_end_time, stream.final_result_end_time, time_diff, offset, len(stream.last_audio_input)))

                # ## debug ##
                if DEBUG is True:
                    with open(stream.debug_out_file + '.pcm', 'ab') as _debug_out_file:
                        _debug_out_file.write(stream.last_audio_input[0:int(offset)])
                    stream.out_bytes += offset
                    with open(stream.debug_out_file + '.' + str(stream.debug_out_file_index) + '.pcm', 'ab') as _debug_out_file_chunk:
                        _debug_out_file_chunk.write(stream.last_audio_input[0:int(offset)])
                # ## debug ##

                stream.last_audio_input = stream.last_audio_input[int(offset):]
                stream.final_result_end_time = new_final_result_end_time

                Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses stream.out_bytes={}, new len(stream.last_audio_input)={}, stream.final_result_end_time={}".format(stream.out_bytes, len(stream.last_audio_input), stream.final_result_end_time))

                Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses stream.current_time={}, stream.start_time={}".format(stream.current_time, stream.start_time))

                if stream.current_time - stream.start_time >= STREAMING_LIMIT:
                    stream.start_time = stream.final_result_end_time
                    Utils.logger_.debug_color('GoogleCloudStreamingTranscribe', "GoogleCloudStreamingTranscribe::_handle_responses limit reached new stream.start_time={}".format(stream.start_time))
                    stream.debug_out_file_index += 1
                    break

    #################################
    # _calculate_time
    #################################
    def _calculate_time(self, _time) -> float:

        sec = 0
        microsec = 0

        if _time.seconds:
            sec = _time.seconds

        if _time.microseconds:
            microsec = _time.microseconds

        return round((sec * 1000) + (microsec / 1000), 2)

    #################################
    # _add_time
    #################################
    def _add_time(self, word_time, time_offset) -> float:

        time_offset_seconds = int(time_offset)
        time_offset_microseconds = (time_offset - time_offset_seconds) * 1e6

        word_time_seconds = 0
        if word_time.seconds:
            word_time_seconds = word_time.seconds

        word_time_microseconds = 0
        if word_time.microseconds:
            word_time_microseconds = word_time.microseconds

        result_time_microseconds = time_offset_microseconds + word_time_microseconds
        result_time_seconds = time_offset_seconds + word_time_seconds

        if result_time_microseconds >= 1e6:
            result_time_seconds += 1
            result_time_microseconds -= 1e6

        return round((int(result_time_seconds) * 1000) + (int(result_time_microseconds) / 1000), 2)

    #################################
    # get_engine_time
    #################################
    def get_engine_time(self) -> float:

        return self._generator.get_engine_time()

    #################################
    # get_engine_accuracy
    #################################
    def get_engine_accuracy(self) -> float:

        if self._engine_accuracy_count == 0:
            return 0

        engine_accuracy = self._engine_accuracy_total / float(self._engine_accuracy_count)
        return engine_accuracy


####################################################
#
#  __main__
#
####################################################
if __name__ == "__main__":
    import io
    import argparse

    parser = argparse.ArgumentParser(description="google_test")
    parser.add_argument('-t', '--text', help="text to translate", type=str, default='')
    parser.add_argument('-f', '--file', help="file to transcribe", type=str, default='')
    parser.add_argument('-s', '--src_lang', help="source language", type=str, default='de')
    parser.add_argument('-d', '--dst_lang', help="destination language", type=str, default='en')
    parser.add_argument('-r', '--sample_rate', help="audio file sampling rate", type=str, default='en')
    args = parser.parse_args()

    g = GoogleCloudApi()

    if args.text != '':
        # text = 'Da stellen sich eine Menge Fragen, auch ethische. Und die hat Michael Spillmann mit der Ethikerin Ruth Baumann-Hölzle besprochen.'
        result = g.translate([args.text], args.src_lang, args.dst_lang, print_result=False)
        print(result[0].translated_text)

    if args.file != '':
        with io.open(args.file, 'rb') as audio_file:
            content = audio_file.read()

        print("len(content): ", len(content))

        gresult = g.transcribe_data(content, int(args.sample_rate), args.src_lang)
        print(gresult)

        for result in gresult.results:
            for alternative in result.alternatives:
                print('Confidence: {}'.format(alternative.confidence))
                print('Transcript: {}'.format(alternative.transcript))
                print('Transcript (reverese): {}'.format(alternative.transcript[::-1]))
