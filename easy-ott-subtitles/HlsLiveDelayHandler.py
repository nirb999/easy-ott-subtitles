import base64
import time
import threading
from typing import List, Tuple, Optional

import m3u8

import Utils as Utils
from CommonTypes import EosHttpConfig, EosFragment, EosUrl, LiveDelayListener
from RequestWrapper import RequestWrapper


####################################################
#
#  HlsLiveDelayHandler
#
####################################################
class HlsLiveDelayHandler(threading.Thread):
    __session_id: str
    __request_wrapper: RequestWrapper
    __open: bool
    __live_origin_manifest_url: str
    __m3u8: Optional[m3u8.model.M3U8]
    __delay_seconds: float
    __first_manifest_read: bool
    __base_media_sequence: int
    __time_in_current_manifest: float
    __current_time: float
    __fragments: List[Tuple[EosFragment, m3u8.model.Segment]]
    __time_in_fragments: float
    __max_media_sequence: int
    __min_fragment_duration: float
    __listeners: List[Tuple[LiveDelayListener, str]]   # (LiveDelayListener, param)
    __lock: threading.RLock

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, session_id: str, live_origin_manifest_url_base64: str, delay_seconds: int):

        self.__session_id = session_id

        self.__open = True

        self.__live_origin_manifest_url = base64.urlsafe_b64decode(live_origin_manifest_url_base64).decode('utf-8')
        Utils.logger_.info(self.__session_id, "HlsLiveDelayHandler::__init__ live_origin_manifest_url={}".format(self.__live_origin_manifest_url))

        self.__request_wrapper = RequestWrapper(self.__session_id, 'HlsLiveDelayHandler ' + self.__live_origin_manifest_url)

        self.__m3u8 = None

        self.__delay_seconds = delay_seconds

        self.__first_manifest_read = True

        self.__base_media_sequence = -1
        self.__time_in_current_manifest = 0
        self.__current_time = 0
        self.__fragments = []
        self.__time_in_fragments = 0
        self.__max_media_sequence = -1
        self.__min_fragment_duration = 1000000.0

        self.__listeners = []

        self.__lock = threading.RLock()

        threading.Thread.__init__(self)  # start thread

    ####################################################
    #  close
    ####################################################
    def close(self) -> None:

        Utils.logger_.info('HlsLiveDelayHandler', "HlsLiveDelayHandler::close")

        self.__open = False

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('HlsLiveDelayHandler', "HlsLiveDelayHandler::run thread started name={}".format(self.getName()))

        while self.__open is True:

            manifest = None
            response = self.__request_wrapper.get(self.__live_origin_manifest_url)
            if response is None:
                Utils.logger_.error(str(self.__session_id), "HlsLiveDelayHandler::run error getting manifest from server")
                time.sleep(1)
                continue

            manifest = response.text

            self.__lock.acquire()

            self.__m3u8 = m3u8.loads(manifest)

            current_media_sequence = -1
            self.__time_in_current_manifest = 0

            self.__base_media_sequence = self.__m3u8.media_sequence

            segments_len = len(self.__m3u8.segments)
            segment_index = 0

            for segment in self.__m3u8.segments:

                segment_index += 1

                self.__time_in_current_manifest += segment.duration

                if current_media_sequence == -1:
                    current_media_sequence = self.__base_media_sequence
                else:
                    current_media_sequence += 1

                if self.__min_fragment_duration > segment.duration:
                    self.__min_fragment_duration = segment.duration

                if current_media_sequence > self.__max_media_sequence:
                    new_fragment = EosFragment()
                    new_fragment.url.set_url(segment.uri, self.__live_origin_manifest_url)
                    new_fragment.media_sequence = current_media_sequence
                    new_fragment.duration = segment.duration
                    new_fragment.start_time = self.__current_time
                    new_fragment.first_read = self.__first_manifest_read

                    if segment.key is not None:
                        new_url = EosUrl()
                        new_url.set_url(segment.key.uri, self.__live_origin_manifest_url)
                        new_fragment.encryption_uri = new_url.absolute_url
                        new_fragment.encryption_iv = segment.key.iv
                        new_fragment.encryption_method = segment.key.method

                    new_fragment.discontinuity = segment.discontinuity

                    if segment_index == segments_len:
                        new_fragment.first_read = False

                    self.__fragments.append((new_fragment, segment))
                    self.__time_in_fragments += segment.duration
                    self.__max_media_sequence = current_media_sequence
                    self.__current_time += segment.duration
                    self.__notify_listeners(new_fragment)
                    # new_fragments_found = True

            if self.__base_media_sequence == -1:
                Utils.logger_.error(str(self.__session_id), "HlsLiveDelayHandler::run EXT-X-MEDIA-SEQUENCE not found")

            if self.__time_in_current_manifest > 60.0:
                if self.__first_manifest_read is True:
                    Utils.logger_.warning(str(self.__session_id), "HlsLiveDelayHandler::run live manifest too long ({} seconds)".format(self.__time_in_current_manifest))
                self.__time_in_current_manifest = 60.0

            if self.__first_manifest_read is True:
                Utils.logger_.debug_color(str(self.__session_id), "HlsLiveDelayHandler::run media_sequence={}".format(self.__base_media_sequence))
                self.__first_manifest_read = False

            self.__lock.release()

            Utils.logger_.debug('HlsLiveDelayHandler', "HlsLiveDelayHandler::run len(self.__fragments)={}, self.__time_in_fragments={}".format(len(self.__fragments), self.__time_in_fragments))
            if self.__time_in_fragments > self.__delay_seconds + 2 * self.__time_in_current_manifest:
                removed = self.__fragments.pop(0)
                self.__time_in_fragments -= removed[0].duration
                Utils.logger_.debug('HlsLiveDelayHandler', "HlsLiveDelayHandler::run removed segment media_sequence={} duration={}".format(removed[0].media_sequence, removed[0].duration))


            #if new_fragments_found is True:
            #    time.sleep(self.__min_fragment_duration * 0.8)
            #else:
            #    time.sleep(self.__min_fragment_duration * 0.2)
            time.sleep(1)

        Utils.logger_.system('HlsLiveDelayHandler', "HlsLiveDelayHandler::run thread ending name={}".format(self.getName()))

    ####################################################
    #  delay
    ####################################################
    def delay(self) -> Tuple[str, List[EosFragment]]:

        if self.__m3u8 is None:
            return '', []

        manifest = ''
        fragment_list: List[EosFragment] = []

        self.__lock.acquire()

        start_index = 0
        end_index = -1
        if self.__time_in_fragments >= self.__delay_seconds + self.__time_in_current_manifest:

            # create delay
            end_index = len(self.__fragments) - 1

            delay_time = 0
            for fragment in reversed(self.__fragments):

                delay_time += fragment[0].duration
                end_index -= 1

                if delay_time >= self.__delay_seconds:
                    break

            # remove fragment from head
            time_in_fragment = self.__time_in_fragments

            for fragment in self.__fragments:

                if time_in_fragment - delay_time > self.__time_in_current_manifest:

                    time_in_fragment -= fragment[0].duration
                    start_index += 1

                else:
                    break

            #for i in range(start_index):
            #    self.__fragments.pop(0)
            #    self.__time_in_fragments -= fragment.duration

        if len(self.__fragments) > start_index:
            self.__m3u8.sequence_number = str(self.__fragments[start_index][0].media_sequence)
        else:
            self.__m3u8.sequence_number = None

        self.__m3u8.segments = m3u8.model.SegmentList()

        if end_index > -1:
            for fragment in self.__fragments[start_index - 1:end_index]:

                fragment[1].uri = fragment[0].url.absolute_url
                self.__m3u8.segments.append(fragment[1])
                fragment_list.append(fragment[0])

        manifest = self.__m3u8.dumps()

        self.__lock.release()

        return manifest, fragment_list

    ####################################################
    #  register_live_parser_listener
    ####################################################
    def register_live_parser_listener(self, listener: LiveDelayListener, param: str) -> None:

        self.__listeners.append((listener, param))

    ####################################################
    #  __notify_listeners
    ####################################################
    def __notify_listeners(self, fragment: EosFragment) -> None:

        for listener, param in self.__listeners:
            listener.on_new_fragment(fragment, param)
