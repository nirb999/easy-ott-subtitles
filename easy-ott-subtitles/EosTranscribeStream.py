import requests
import subprocess
import io
import os
import datetime
import re
import difflib
import copy
import shlex
import time
import threading
import queue
from urllib.parse import urlparse, urlunparse
from typing import Optional, List, Dict, Any
import hashlib

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

import Utils as Utils
from GoogleCloudApi import GoogleCloudStreamingTranscribe, GoogleCloudStreamingGenerator, GoogleCloudApiListener
from RevAiApi import RevAitreamingTranscribe
import Transcoder as Transcoder
from CommonTypes import EosFragmentEncodings, EosHttpConfig, EosFragment, LiveDelayListener
from Languages import EosLanguage
from OttHandler import OttProtocols
from DashUtils import DashFragmentDecoder
from RequestWrapper import RequestWrapper
# from EosFragment import EosFragment


# config variables
APP__TMP_FILES_PATH = Utils.ConfigVariable('APP', 'TMP_FILES_PATH', type=str, default_value='temp', description='Path to temporary files directory', mandatory=True)


####################################################
#
#  StreamingTranscribeWriter
#
####################################################
class StreamingTranscribeWriter(threading.Thread, GoogleCloudApiListener):
    __queue: queue.Queue
    __subs: Dict[str, List[Dict[str, Any]]]  # language -> list of subs
    __time_in_subs: Dict[str, float]  # language -> time
    __initial_time_offset: float
    __is_live: bool

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, src_language: EosLanguage, dst_languages: List[EosLanguage], is_live: bool):

        self.__queue = queue.Queue()

        self.__initial_time_offset = 0

        self.__subs = {}
        self.__time_in_subs = {}

        self.__is_live = is_live

        for lang in dst_languages:
            self.__subs[lang.code_bcp_47()] = []
            self.__time_in_subs[lang.code_bcp_47()] = 0

        if src_language.code_bcp_47() not in self.__subs.keys():
            self.__subs[src_language.code_bcp_47] = []
            self.__time_in_subs[src_language.code_bcp_47] = 0

        GoogleCloudApiListener.__init__(self, src_language, dst_languages)
        threading.Thread.__init__(self, name='StreamingTranscribeWriter')  # start thread

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('********************************************** StreamingTranscribeWriter', "StreamingTranscribeWriter::run thread started name={}".format(self.getName()))

        while True:

            Utils.logger_.debug_color('StreamingTranscribeWriter', "StreamingTranscribeWriter::run loop name={}".format(self.getName()))

            words_ = self.__queue.get()

            self._words_to_sentences(words_)

        Utils.logger_.system('StreamingTranscribeWriter', "StreamingTranscribeWriter::run thread ending name={}".format(self.getName()))

    #################################
    # _handle_text
    #################################
    def _handle_text(self, dst_language: EosLanguage, lines: List[Dict[str, Any]]) -> None:

        #print("********************************************** StreamingTranscribeWriter::__handle_text start_time:{}, end_time:{}, text:{}".format(lines))

        if len(lines) == 0:
            return

        text = ''
        for line in lines:
            text += line['text'] + '\n'

        time = lines[-1]['end'] - lines[0]['start']
        #print("**************************************************************** time=", time)

        self.__subs[dst_language.code_bcp_47()].append({'start': lines[0]['start'], 'end': lines[-1]['end'], 'text': text})

        self.__time_in_subs[dst_language.code_bcp_47()] += time

        if self.__is_live:
            while self.__time_in_subs[dst_language.code_bcp_47()] > 140:
                sub = self.__subs[dst_language.code_bcp_47()].pop(0)
                sub_time = sub['end'] - sub['start']
                self.__time_in_subs[dst_language.code_bcp_47()] -= sub_time
                Utils.logger_.debug('StreamingTranscribeWriter', "StreamingTranscribeWriter::_handle_text live removing sub __time_in_subs[{}]={}".format(dst_language.code_bcp_47(), self.__time_in_subs[dst_language.code_bcp_47()]))

    #################################
    # handle_words
    #################################
    def handle_words(self, words: List, time_offset: float) -> None:

        self.__queue.put({'words': words, 'time_offset': time_offset + self.__initial_time_offset})

    #################################
    # get_subs
    #################################
    def get_subs(self, dst_lang: str) -> List[Dict[str, Any]]:

        return self.__subs[dst_lang]

    #################################
    # set_initial_time_offset
    #################################
    def set_initial_time_offset(self, time_offset: float) -> None:

        self.__initial_time_offset = time_offset


####################################################
#
#  EosTranscribeStreamBase
#
####################################################
class EosTranscribeStreamBase(threading.Thread):

    _session_id: str
    _ott_protocol: OttProtocols
    _src_language: EosLanguage
    _dst_languages: List[EosLanguage]
    _sample_rate: int
    _listener: StreamingTranscribeWriter
    _audio_generator: GoogleCloudStreamingGenerator
    _open: bool
    _hls_key_request_wrapper: RequestWrapper
    _paused: bool
    _pending_pause: bool
    _pending_resume: bool
    _pending_open: bool
    _pending_close: bool
    _ready: threading.Event
    _google_streaming: GoogleCloudStreamingTranscribe
    _rev_ai_streaming: RevAitreamingTranscribe
    _delete_tmp_files: bool

    #################################
    # __init__
    #################################
    def __init__(self,
                 session_id: str,
                 ott_protocol: OttProtocols,
                 src_language: EosLanguage,
                 dst_languages: List[EosLanguage],
                 sample_rate: int) -> None:

        Utils.logger_.system('EosTranscribeStreamBase', "EosTranscribeStreamBase::__init__ src_language={}".format(src_language))

        self._session_id = session_id
        self._ott_protocol = ott_protocol
        self._src_language = src_language
        self._dst_languages = dst_languages
        self._sample_rate = sample_rate

        self._hls_key_request_wrapper = RequestWrapper(session_id, 'EosTranscribeStreamBase HLS key')
        self._hls_key_request_wrapper.use_last_response()

        self._open = True

        self._listener = StreamingTranscribeWriter(self._src_language, self._dst_languages, self._live())
        self._listener.start()

        self._paused = False
        self._pending_pause = False
        self._pending_resume = False
        self._pending_close = False
        self._pending_open = True

        self._audio_generator = GoogleCloudStreamingGenerator(self._sample_rate)

        self._ready = threading.Event()
        self._google_streaming = GoogleCloudStreamingTranscribe(self._ready, self._src_language, self._sample_rate, self._audio_generator, self._listener)
        self._google_streaming.start()
        #self._rev_ai_streaming = RevAitreamingTranscribe(self._ready, src_language, self._sample_rate, self._audio_generator, self._listener)
        #self._rev_ai_streaming.start()
        self._ready.wait()
        self._ready.clear()

        self._pending_open = False

        self._delete_tmp_files = True

        threading.Thread.__init__(self, name='EosTranscribeStreamBase')  # start thread

    #################################
    # _live
    #################################
    def _live(self) -> bool:
        return False

    #################################
    # get_subs
    #################################
    def get_subs(self, dst_lang: str) -> List[Dict[str, Any]]:

        return self._listener.get_subs(dst_lang)

    #################################
    # pause
    #################################
    def pause(self) -> None:

        Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::pause")

        if self._paused is True:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::pause already in pause")
            return

        if self._pending_open is True:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::pause _pending_open is True")
            self._pending_pause = True
            return

        self._paused = True
        self._pending_close = True

        self._audio_generator.close()

        self._ready.wait()
        self._ready.clear()

        self._audio_generator = None
        self._google_streaming = None
        #self._rev_ai_streaming = None

        self._pending_close = False

        Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::pause completed")

        if self._pending_resume is True:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::pause _pending_resume is True")
            self.resume()

    #################################
    # resume
    #################################
    def resume(self) -> None:

        Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::resume")

        if self._paused is False:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::resume already in resume")
            return

        if self._pending_close is True:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::resume _pending_close is True")
            self._pending_resume = True
            return

        self._paused = False
        self._pending_open = True

        self._reset_params()

        self._audio_generator = GoogleCloudStreamingGenerator(self._sample_rate)

        self._google_streaming = GoogleCloudStreamingTranscribe(self._ready, self._src_language, self._sample_rate, self._audio_generator, self._listener)
        self._google_streaming.start()
        #self._rev_ai_streaming = RevAitreamingTranscribe(self._ready, src_language, self._sample_rate, self._audio_generator, self._listener)
        #self._rev_ai_streaming.start()
        self._ready.wait()
        self._ready.clear()

        self._pending_open = False

        Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::resume completed")

        if self._pending_pause is True:
            Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::resume _pending_pause is True")
            self.pause()

    #################################
    # close
    #################################
    def close(self) -> None:

        self._open = False

    #################################
    # _reset_params
    #################################
    def _reset_params(self) -> None:
        pass

    #################################
    # get_state
    #################################
    def get_state(self) -> str:

        if self._paused is True:
            return 'paused'
        return 'active'

    #################################
    # get_engine_time
    #################################
    def get_engine_time(self) -> float:
        if self._google_streaming is None:
            return 0

        return self._google_streaming.get_engine_time()

    #################################
    # get_engine_accuracy
    #################################
    def get_engine_accuracy(self) -> float:
        if self._google_streaming is None:
            return 0

        return self._google_streaming.get_engine_accuracy()

    #################################
    # _decrypt_hls
    #################################
    def _decrypt_hls(self, fragment: EosFragment, original_fragment: bytes) -> bytes:

        Utils.logger_.debug('EosTranscribeStreamBase', "EosTranscribeStreamBase::_decrypt_hls")

        key = None
        response = self._hls_key_request_wrapper.get(fragment.encryption_uri)
        if response is None:
            Utils.logger_.error(str(self._session_id), "EosTranscribeStreamBase::_decrypt_hls error getting manifest from server")
            return

        key = response.content

        #print("fragment.encryption_iv:", fragment.encryption_iv)

        iv = bytes.fromhex(fragment.encryption_iv[2:])
        #print("iv:", iv)

        if fragment.encryption_method == 'AES-128':

            cipher = AES.new(key, AES.MODE_CBC, iv=iv)  # Setup cipher
            decrypted = unpad(cipher.decrypt(original_fragment), AES.block_size)  # Decrypt and then up-pad the result
            original_fragment = decrypted

        return original_fragment


####################################################
#
#  EosTranscribeStream
#
####################################################
class EosTranscribeStream(EosTranscribeStreamBase):

    #################################
    # __init__
    #################################
    def __init__(self,
                 session_id: str,
                 ott_protocol: OttProtocols,
                 src_language: EosLanguage,
                 dst_languages: List[EosLanguage],
                 fragemnts_list: List[EosFragment],
                 sample_rate: int) -> None:

        Utils.logger_.system('EosTranscribeStream', "EosTranscribeStream::__init__")

        self._fragemnts_list = fragemnts_list

        EosTranscribeStreamBase.__init__(self, session_id, ott_protocol, src_language, dst_languages, sample_rate)

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('EosTranscribeStream', "EosTranscribeStream::run thread started name={}".format(self.getName()))

        total_bytes = 0

        for fragment in self._fragemnts_list:

            print(fragment)

            try:
                headers = {'User-Agent': EosHttpConfig.user_agent}
                response = requests.get(fragment.url.absolute_url, headers=headers)

                if response.status_code == requests.codes.ok:

                    original_fragment = response.content
                    Utils.logger_.dump(str(self._session_id), 'EosTranscribeStream::run len(original_fragment)={}'.format(len(original_fragment)))

                else:
                    Utils.logger_.error(str(self._session_id), "EosTranscribeStream::run error getting fragment from server ({}) url={}".format(response.status_code, original_fragment_url))
                    return

            except requests.ConnectionError:
                error_str = "EosTranscribeStream::run Error connecting to server {}".format(fragment.absolte_url)
                Utils.logger_.error(str(self._session_id), error_str)
                return

            # decrypt if needed
            if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
                if fragment.encryption_uri != '':
                    original_fragment = self._decrypt_hls(fragment, original_fragment)

            original_file_name = APP__TMP_FILES_PATH.value() + '/' + hashlib.md5(fragment.url.base64_urlsafe.encode('utf-8')).hexdigest()
            with open(original_file_name, 'wb') as original_file:
                original_file.write(original_fragment)

            audio_file = original_file_name + '.aac'
            if self._ott_protocol == OttProtocols.DASH_PROTOCOL:
                dash_decoder = DashFragmentDecoder(original_file_name)
                dash_decoder.read_aac(audio_file, fragment.sampling_rate)

            if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
                Transcoder.transcoder_.extract_audio(original_file_name, audio_file)

            pcm_file = original_file_name + '.pcm'
            Transcoder.transcoder_.transcode_file(audio_file,
                                                  pcm_file,
                                                  self._sample_rate)

            with open(pcm_file, 'rb') as pcm:
                audio_data = pcm.read()
                print("len(audio_data) = {}".format(len(audio_data)))
                #self._audio_generator.put_fragment(audio_data)

                # ## debug ##
                with open("in_pcm.pcm", 'ab') as _debug_in_file:
                    _debug_in_file.write(audio_data)
                # ## debug ##

                # cut in to 500ms chunks
                index = 0
                chunk_size = int(2 * self._sample_rate / 2)
                print("before index={}, chunk_size={}, len(audio_data)={}".format(index, chunk_size, len(audio_data)))
                while index < len(audio_data):
                    if chunk_size > len(audio_data) - index:
                        chunk_size = len(audio_data) - index
                    self._audio_generator.put_fragment(audio_data[index:(index + chunk_size)])
                    index += chunk_size
                    total_bytes += chunk_size
                    print("inside index={}, chunk_size={}, len(audio_data)={}, total_bytes={}".format(index, chunk_size, len(audio_data), total_bytes))

                    time.sleep(chunk_size / (2 * self._sample_rate) * 0.6)

                print("after index={}, chunk_size={}, len(audio_data)={}, total_bytes={}".format(index, chunk_size, len(audio_data), total_bytes))

            if self._delete_tmp_files is True:
                if os.path.exists(original_file_name):
                    os.remove(original_file_name)
                if os.path.exists(audio_file):
                    os.remove(audio_file)
                if os.path.exists(pcm_file):
                    os.remove(pcm_file)

        # signal end of stream
        self._audio_generator.put_fragment(None)


####################################################
#
#  EosTranscribeLiveStream
#
####################################################
class EosTranscribeLiveStream(EosTranscribeStreamBase, LiveDelayListener):

    __queue: queue.Queue
    _base_pts: int
    _base_start_time: float
    __request_wrapper: RequestWrapper

    #################################
    # __init__
    #################################
    def __init__(self,
                 session_id: str,
                 ott_protocol: OttProtocols,
                 src_language: EosLanguage,
                 dst_languages: List[EosLanguage],
                 sample_rate: int) -> None:

        Utils.logger_.system('EosTranscribeLiveStream', "EosTranscribeLiveStream::__init__")

        self._first_fragment_read = True

        self._base_pts = 0
        self._base_start_time = 0

        self.__queue = queue.Queue()

        self.__request_wrapper = RequestWrapper(session_id, 'EosTranscribeLiveStream')

        EosTranscribeStreamBase.__init__(self, session_id, ott_protocol, src_language, dst_languages, sample_rate)

    #################################
    # _live
    #################################
    def _live(self) -> bool:
        return True

    #################################
    # _reset_params
    #################################
    def _reset_params(self) -> None:

        self._first_fragment_read = True

        self._base_pts = 0
        self._base_start_time = 0

    #################################
    # on_new_fragment
    # derived from LiveDelayListener
    #################################
    def on_new_fragment(self, fragment: EosFragment, param: str) -> None:
        # Utils.logger_.debug('EosTranscribeLiveStream', "EosTranscribeLiveStream::on_new_fragment fragment={}".format(fragment))

        if self._paused is True:
            return

        self.__queue.put(fragment)

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('EosTranscribeLiveStream', "EosTranscribeLiveStream::run thread started name={}".format(self.getName()))

        total_bytes = 0

        target_time: datetime.datetime = datetime.datetime.now()

        while self._open is True:

            fragment: EosFragment = self.__queue.get()

            if fragment.first_read is True:
                # Utils.logger_.debug('EosTranscribeLiveStream', "first_read=True, ignoring fragment")
                continue

            if self._first_fragment_read is True:
                target_time = datetime.datetime.now()

            Utils.logger_.debug_color('EosTranscribeLiveStream', "reading fragment {}".format(fragment.url.absolute_url))

            dl_start_time = datetime.datetime.now()

            original_fragment = None
            response = self.__request_wrapper.get(fragment.url.absolute_url)
            if response is None:
                Utils.logger_.error(str(self._session_id), "EosTranscribeLiveStream::run error getting fragment from server: {}".format(fragment.url.absolute_url))
                continue

            original_fragment = response.content

            dl_end_time = datetime.datetime.now()
            dl_time = dl_end_time - dl_start_time

            Utils.logger_.debug_color('EosTranscribeLiveStream', "fragment {}: dl_time={}".format(fragment.url.absolute_url, dl_time))

            transcode_start_time = datetime.datetime.now()

            #print("fragment: ", fragment)

            # decrypt if needed
            if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
                if fragment.encryption_uri != '':
                    original_fragment = self._decrypt_hls(fragment, original_fragment)

            original_file_name = APP__TMP_FILES_PATH.value() + '/' + hashlib.md5(fragment.url.base64_urlsafe.encode('utf-8')).hexdigest()
            with open(original_file_name, 'wb') as original_file:
                original_file.write(original_fragment)

            first_video_pts = -1
            if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
                first_video_pts = Transcoder.transcoder_.get_first_pts(original_file_name)
                Utils.logger_.dump('EosTranscribeLiveStream', "first_video_pts = {}".format(first_video_pts))

                # if self._last_hls_fragment_pts + self._last_hls_fragment_duration != first_video_pts:   

            if fragment.discontinuity is True:
                self._base_pts = first_video_pts
                self._base_start_time = fragment.start_time
                Utils.logger_.info('EosTranscribeLiveStream', "self._base_pts={}".format(self._base_pts))

            if self._first_fragment_read is True:
                self._first_fragment_read = False
                self._listener.set_initial_time_offset(fragment.start_time)
                self._base_pts = first_video_pts
                self._base_start_time = fragment.start_time
                Utils.logger_.info('EosTranscribeLiveStream', "self._base_pts={}".format(self._base_pts))

            audio_file = original_file_name + '.aac'
            if self._ott_protocol == OttProtocols.DASH_PROTOCOL:
                dash_decoder = DashFragmentDecoder(original_file_name)
                dash_decoder.read_aac(audio_file, fragment.sampling_rate)

            if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
                Transcoder.transcoder_.extract_audio(original_file_name, audio_file)

            pcm_file = original_file_name + '.pcm'
            Transcoder.transcoder_.transcode_file(audio_file,
                                                  pcm_file,
                                                  self._sample_rate)

            transcode_end_time = datetime.datetime.now()
            transcode_time = transcode_end_time - transcode_start_time
            total_process_time = dl_time + transcode_time

            Utils.logger_.debug_color('EosTranscribeLiveStream', "fragment {}: transcode_time={}, total_process_time={}".format(fragment.url.absolute_url, transcode_time, total_process_time))

            try:
                pcm = open(pcm_file, 'rb')
            except OSError:
                Utils.logger_.error('EosTranscribeLiveStream', 'error opening file {}'.format(pcm_file))
                if self._delete_tmp_files is True:
                    if os.path.exists(original_file_name):
                        os.remove(original_file_name)
                    if os.path.exists(audio_file):
                        os.remove(audio_file)
                    if os.path.exists(pcm_file):
                        os.remove(pcm_file)
                continue

            audio_data = pcm.read()
            Utils.logger_.dump('EosTranscribeLiveStream', "len(audio_data) = {}".format(len(audio_data)))

            # ## debug ##
            # with open("in_pcm.pcm", 'ab') as _debug_in_file:
            #     _debug_in_file.write(audio_data[index:(index + chunk_size)])
            # ## debug ##

            fragment_time = datetime.timedelta(seconds=(float(len(audio_data)) / float(2 * self._sample_rate)))
            target_time = target_time + fragment_time
            Utils.logger_.debug_color('EosTranscribeLiveStream', "fragment {}: fragment_time={}, target_time={}".format(fragment.url.absolute_url, fragment_time, target_time))

            # cut it to 100ms chunks
            index = 0
            chunk_size = int(2 * self._sample_rate / 2)
            Utils.logger_.dump('EosTranscribeLiveStream', "before index={}, chunk_size={}, len(audio_data)={}".format(index, chunk_size, len(audio_data)))
            while index < len(audio_data):
                if chunk_size > len(audio_data) - index:
                    chunk_size = len(audio_data) - index
                if self._audio_generator is not None:
                    self._audio_generator.put_fragment(audio_data[index:(index + chunk_size)])                    
                index += chunk_size
                total_bytes += chunk_size
                Utils.logger_.dump('EosTranscribeLiveStream', "inside index={}, chunk_size={}, len(audio_data)={}, total_bytes={}".format(index, chunk_size, len(audio_data), total_bytes))

                bytes_left = len(audio_data) - index
                if bytes_left > 0:
                    current_time = datetime.datetime.now()
                    if target_time > current_time:
                        time_left = target_time - current_time
                        rate = time_left.total_seconds() / float(bytes_left)
                        time.sleep(rate * chunk_size)

                        #time.sleep(chunk_size / (2 * self._sample_rate) * 0.6)

            pcm.close()

            Utils.logger_.dump('EosTranscribeLiveStream', "after index={}, chunk_size={}, len(audio_data)={}, total_bytes={}".format(index, chunk_size, len(audio_data), total_bytes))
            Utils.logger_.debug_color('EosTranscribeLiveStream', "fragment {} done".format(fragment.url.absolute_url))

            if self._delete_tmp_files is True:
                if os.path.exists(original_file_name):
                    os.remove(original_file_name)
                if os.path.exists(audio_file):
                    os.remove(audio_file)
                if os.path.exists(pcm_file):
                    os.remove(pcm_file)

    #################################
    # get_subs
    #################################
    def get_start_times(self):

        return self._base_pts, self._base_start_time
