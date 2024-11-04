import base64
import time
import threading
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from typing import List, Tuple, Dict, Optional, Any

import mpegdash.parser
import isodate

import Utils as Utils
from CommonTypes import EosNames, EosHttpConfig, EosFragment, EosUrl, LiveDelayListener
from Languages import EosLanguage
from DashUtils import DashInitDecoder
from RequestWrapper import RequestWrapper


####################################################
#  DashLiveDelayStream
####################################################
class DashLiveDelayStream:
    time_in_current_manifest: float
    current_time: float
    fragments: List[EosFragment]
    time_in_fragments: float
    max_timestamp: int
    time_scale: float
    media: str
    presentation_time_offset: int
    audio_sampling_rate: int

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:
        self.max_timestamp = -1
        self.time_in_current_manifest = 0
        self.current_time = 0
        self.fragments = []
        self.time_in_fragments = 0
        self.time_scale = 1
        self.media = ''
        self.presentation_time_offset = 0
        self.audio_sampling_rate = 0


####################################################
#
#  DashLiveDelayHandler
#
####################################################
class DashLiveDelayHandler(threading.Thread):
    __session_id: str
    __live_origin_manifest_url: str
    __delay_seconds: float
    __first_manifest_read: bool
    __mpd_buffer_time_set: bool
    __reference_adaptation_set_id: Optional[str]
    __streams: Dict[Any, DashLiveDelayStream]  # (content_type, adaptation_set_id) -> DashLiveDelayStream
    __eos_streams: List[int]  # adaptation_set_id
    __listeners: List[Tuple[LiveDelayListener, str]]   # (LiveDelayListener, param)
    __mpd: Optional[mpegdash.nodes.MPEGDASH]
    __base_urls: List[str]
    __lock: threading.RLock
    #__mpd: Optional[ET.Element]
    __ready: threading.Event
    __request_wrapper: RequestWrapper

    ####################################################
    #  __init__
    ####################################################
    def __init__(self, session_id: str, live_origin_manifest_url: str, delay_seconds: int, ready: threading.Event) -> None:

        self.__session_id = session_id

        self.__live_origin_manifest_url = live_origin_manifest_url

        Utils.logger_.info(self.__session_id, "DashLiveDelayHandler::__init__ live_origin_manifest_url={}".format(self.__live_origin_manifest_url))

        self.__request_wrapper = RequestWrapper(self.__session_id, 'DashLiveDelayHandler ' + self.__live_origin_manifest_url)

        self.__delay_seconds = delay_seconds

        self.__first_manifest_read = True
        self.__mpd_buffer_time_set = False

        self.__reference_adaptation_set_id = None

        self.__streams = {}
        self.__eos_streams = []

        self.__listeners = []

        self.__mpd = None

        self.__base_urls = []

        self.__ready = ready

        self.__lock = threading.RLock()

        threading.Thread.__init__(self)  # start thread

    ####################################################
    #  set_reference_stream
    ####################################################
    def set_reference_stream(self, reference_adaptation_set_id: int, next_adaptation_set_id: int, dst_language: EosLanguage, default_lang: EosLanguage):

        Utils.logger_.info('DashLiveDelayHandler', "DashLiveDelayHandler::set_reference_stream reference_adaptation_set_id={}, next_adaptation_set_id={}, dst_language={}".format(reference_adaptation_set_id, next_adaptation_set_id, dst_language.code_bcp_47()))

        self.__reference_adaptation_set_id = reference_adaptation_set_id

        #reference_adaptation_set = None
        #period = self.__mpd.periods[0]
        #for ref_adaptation_set in period.adaptation_sets:
        #    if ref_adaptation_set.id == reference_adaptation_set_id:
        #        reference_adaptation_set = ref_adaptation_set

        # create new adaptation set for the dst_language
        adaptation_set = mpegdash.nodes.AdaptationSet()
        adaptation_set.id = next_adaptation_set_id
        adaptation_set.group = 3 # 8
        #adaptation_set.bitstream_switching = True
        #adaptation_set.segment_alignment = True
        adaptation_set.content_type = "text"
        adaptation_set.codecs = "stpp"
        adaptation_set.mime_type = "application/mp4"
        adaptation_set.start_with_sap = 1
        adaptation_set.lang = dst_language.code_639_1()

        if True:
            adaptation_set.id = None
            adaptation_set.content_type = None
            adaptation_set.codecs = None

            content_component = mpegdash.nodes.ContentComponent()
            content_component.id = '1'
            content_component.content_type = "text"
            adaptation_set.content_components = []
            adaptation_set.content_components.append(content_component)

        role = mpegdash.nodes.Descriptor()
        role.scheme_id_uri = "urn:mpeg:dash:role:2011"
        role.value = "subtitle"
        adaptation_set.roles = [role]

        representation = mpegdash.nodes.Representation()
        representation.id = "eos"
        representation.bandwidth = 8000

        if True:
            representation.codecs = "stpp"
            representation.id = next_adaptation_set_id + 2

            sub_representation = mpegdash.nodes.SubRepresentation()
            sub_representation.bandwidth = 8000
            sub_representation.codecs = "stpp"
            sub_representation.content_component = 1
            representation.sub_representations = sub_representation

        adaptation_set.representations = [representation]

        # reference_segment_template = reference_adaptation_set.segment_templates[0]

        segment_template = mpegdash.nodes.SegmentTemplate()
        segment_template.timescale = 1000  # reference_segment_template.timescale
        segment_template.media = "{}/{}/$Time$".format(EosNames.fragment_dash_prefix, dst_language.code_bcp_47())
        segment_template.initialization = "{}/{}/Init".format(EosNames.fragment_dash_prefix, dst_language.code_bcp_47())

        segment_timeline = mpegdash.nodes.SegmentTimeline()

        segment_template.segment_timelines = [segment_timeline]
        adaptation_set.segment_templates = [segment_template]

        self.__mpd.periods[0].adaptation_sets.append(adaptation_set)
        self.__eos_streams.append(next_adaptation_set_id)

    ####################################################
    #  get_presentation_time_offset
    ####################################################
    def get_presentation_time_offset(self) -> int:
        return self.__streams[('audio', self.__reference_adaptation_set_id)].presentation_time_offset

    ####################################################
    # run
    # called from thread context when start() is called
    ####################################################
    def run(self) -> None:

        Utils.logger_.system('DashLiveDelayHandler', "DashLiveDelayHandler::run thread started name={}".format(self.getName()))

        while True:

            original_manifest = None
            response = self.__request_wrapper.get(self.__live_origin_manifest_url)
            if response is None:
                Utils.logger_.error(str(self.__session_id), "DashLiveDelayHandler::run error getting manifest from server")
                time.sleep(1)
                continue

            original_manifest = response.text

            # print(original_manifest)

            self.__lock.acquire()

            mpd = mpegdash.parser.MPEGDASHParser.parse(original_manifest)

            if self.__mpd is None:
                self.__mpd = mpd

            if mpd.base_urls is not None:
                self.__base_urls.append(mpd.base_urls[0].base_url_value)
                mpd.base_urls = None

            period = mpd.periods[0]

            if period.base_urls is not None:
                self.__base_urls.append(period.base_urls[0].base_url_value)
                period.base_urls = None

            for adaptation_set in period.adaptation_sets:

                adaptation_set_id = adaptation_set.id
                content_type = adaptation_set.content_type

                if adaptation_set_id is None:
                    if adaptation_set.content_components is not None:
                        adaptation_set_id = adaptation_set.content_components[0].id
                        content_type = adaptation_set.content_components[0].content_type

                stream_key = (content_type, adaptation_set_id)
                # print("stream_key=", stream_key)

                if stream_key not in self.__streams.keys():
                    self.__streams[stream_key] = DashLiveDelayStream()

                if adaptation_set.audio_sampling_rate is not None:
                    self.__streams[stream_key].audio_sampling_rate = int(adaptation_set.audio_sampling_rate)

                self.__streams[stream_key].time_in_current_manifest = 0

                min_bandwidth = 1e12
                representation_id = ''
                for representation in adaptation_set.representations:
                    if representation.audio_sampling_rate is not None:
                        self.__streams[stream_key].audio_sampling_rate = int(representation.audio_sampling_rate)
                    if representation.bandwidth < min_bandwidth:
                        min_bandwidth = representation.bandwidth
                        representation_id = representation.id

                #Utils.logger_.debug('DashLiveDelayHandler', "DashLiveDelayHandler::run adaptation_set_id={}, content_type={}, min_bandwidth={}, audio_sampling_rate={}".format(adaptation_set_id, content_type, min_bandwidth, self.__streams[adaptation_set_id].audio_sampling_rate))

                segment_template = adaptation_set.segment_templates[0]

                new_url = EosUrl()
                media = segment_template.media
                if len(self.__base_urls) > 0:
                    media = self.__base_urls[-1] + media
                new_url.set_url(media, self.__live_origin_manifest_url)
                segment_template.media = new_url.absolute_url

                new_url = EosUrl()
                initialization = segment_template.initialization
                if len(self.__base_urls) > 0:
                    initialization = self.__base_urls[-1] + initialization
                new_url.set_url(initialization, self.__live_origin_manifest_url)
                segment_template.initialization = new_url.absolute_url
                audio_init_url = segment_template.initialization

                if content_type == "audio" and self.__streams[stream_key].audio_sampling_rate == 0:
                    # we need to read audio init fragment to get the audio_sampling_rate
                    audio_init_url = audio_init_url.replace('$RepresentationID$', representation_id)
                    self.__streams[stream_key].audio_sampling_rate = self._read_audio_init(audio_init_url)

                #from urllib.parse import urlparse, urlunparse
                #base_url = mpegdash.nodes.BaseURL()
                #parsed_parent_url = urlparse(self.__live_origin_manifest_url)
                #new_path = parsed_parent_url.path[:parsed_parent_url.path.rfind('/') + 1]
                #base = urlunparse((parsed_parent_url.scheme,
                #                   parsed_parent_url.netloc,
                #                   new_path,
                #                   '',
                #                   '',
                #                   ''))
                #base_url.base_url_value = base
                #adaptation_set.base_urls = []
                #adaptation_set.base_urls.append(base_url)

                self.__streams[stream_key].time_scale = segment_template.timescale
                self.__streams[stream_key].media = segment_template.media

                if self.__first_manifest_read is True:
                    Utils.logger_.debug('DashLiveDelayHandler', "DashLiveDelayHandler::run adaptation_set_id={}, content_type={}, time_scale={}, media={}".format(adaptation_set_id, content_type, self.__streams[stream_key].time_scale, self.__streams[stream_key].media))

                segment_time_line = segment_template.segment_timelines[0]

                for s in segment_time_line.Ss:
                    #print(s.t, s.d, s.r)

                    duration: int = 0
                    repeat: int = 1

                    if s.t is not None:
                        current_timestamp = s.t

                    if s.d is not None:
                        duration = float(s.d)
                        # next_timestamp = current_timestamp + duration

                    if s.r is not None:
                        repeat = s.r + 1

                    for x in range(repeat):

                        self.__streams[stream_key].time_in_current_manifest += (duration / self.__streams[stream_key].time_scale)

                        if current_timestamp > self.__streams[stream_key].max_timestamp:

                            new_media = self.__streams[stream_key].media
                            new_media = new_media.replace('$Bandwidth$', str(min_bandwidth))
                            new_media = new_media.replace('$Time$', str(current_timestamp))
                            new_media = new_media.replace('$RepresentationID$', representation_id)

                            new_fragment = EosFragment()
                            new_fragment.url.set_url(new_media, self.__live_origin_manifest_url)
                            new_fragment.sampling_rate = self.__streams[stream_key].audio_sampling_rate
                            new_fragment.timestamp = current_timestamp
                            new_fragment.duration = (duration / self.__streams[stream_key].time_scale)
                            new_fragment.start_time = self.__streams[stream_key].current_time
                            new_fragment.first_read = self.__first_manifest_read
                            #if line_index == len(lines):
                            #    new_fragment.first_read = False

                            self.__streams[stream_key].fragments.append(new_fragment)
                            self.__streams[stream_key].time_in_fragments += (duration / self.__streams[stream_key].time_scale)
                            self.__streams[stream_key].max_timestamp = current_timestamp
                            self.__streams[stream_key].current_time += (duration / self.__streams[stream_key].time_scale)
                            if self.__streams[stream_key].presentation_time_offset == 0:
                                self.__streams[stream_key].presentation_time_offset = new_fragment.timestamp / self.__streams[stream_key].time_scale

                            if content_type == "audio" and self.__reference_adaptation_set_id is not None and self.__reference_adaptation_set_id == adaptation_set_id:
                                self.__notify_listeners(new_fragment)

                            #print("+++++++++++ adaptation_set_id={}, fragments={}, time_in_fragments={}".format(adaptation_set_id, len(self.__streams[stream_key].fragments), self.__streams[stream_key].time_in_fragments))

                        current_timestamp += int(duration)

                if self.__streams[stream_key].time_in_current_manifest > 60.0:
                    if self.__first_manifest_read is True:
                        Utils.logger_.warning(self.__session_id, "DashLiveDelayHandler::run live manifest too long ({} seconds)".format(self.__streams[stream_key].time_in_current_manifest))
                    self.__streams[stream_key].time_in_current_manifest = 60.0

            if self.__first_manifest_read is True:
                # Utils.logger_.debug_color(self.__session_id, "DashLiveDelayHandler::run media_sequence={}".format(self.__base_media_sequence))
                self.__first_manifest_read = False

            self.__lock.release()

            self.__ready.set()

            time.sleep(1)

        Utils.logger_.system('DashLiveDelayHandler', "DashLiveDelayHandler::run thread ending name={}".format(self.getName()))

    ####################################################
    #  delay
    ####################################################
    def delay(self) -> Tuple[str, List[EosFragment]]:

        fragment_list: List[EosFragment] = []

        self.__lock.acquire()

        #copy_segment_time_line = None
        reference_timescale = 0
        reference_start_time = 0
        reference_duration = 0

        for stream in self.__streams:

            #print("++++++++++++++++++++++ stream: ", stream)

            start_index = 0
            end_index = -1

            #print("++++++++++++++++++++++ self.__streams[stream].time_in_fragments: ", self.__streams[stream].time_in_fragments)
            #print("++++++++++++++++++++++ self.__delay_seconds: ", self.__delay_seconds)
            #print("++++++++++++++++++++++ self.__streams[stream].time_in_current_manifest: ", self.__streams[stream].time_in_current_manifest)

            if self.__streams[stream].time_in_fragments >= self.__delay_seconds + self.__streams[stream].time_in_current_manifest:

                # create delay
                end_index = len(self.__streams[stream].fragments) - 1

                delay_time = 0
                for fragment in reversed(self.__streams[stream].fragments):

                    delay_time += fragment.duration
                    end_index -= 1

                    if delay_time >= self.__delay_seconds:
                        break

                # remove fragment from head
                time_in_fragment = self.__streams[stream].time_in_fragments

                for fragment in self.__streams[stream].fragments:

                    if time_in_fragment - delay_time > self.__streams[stream].time_in_current_manifest:

                        time_in_fragment -= fragment.duration
                        start_index += 1

                    else:
                        break

                #for i in range(start_index):
                #    self.__fragments.pop(0)
                #    self.__time_in_fragments -= fragment.duration

            #print("******************** start_index={}, end_index={}".format(start_index, end_index))

            period = self.__mpd.periods[0]
            for adaptation_set in period.adaptation_sets:

                adaptation_set_id = adaptation_set.id
                content_type = adaptation_set.content_type

                if adaptation_set_id is None:
                    if adaptation_set.content_components is not None:
                        adaptation_set_id = adaptation_set.content_components[0].id
                        content_type = adaptation_set.content_components[0].content_type

                if (content_type, adaptation_set_id) == stream:

                    segment_template = adaptation_set.segment_templates[0]

                    segment_time_line = segment_template.segment_timelines[0]

                    segment_time_line.Ss.clear()

                    #print("******************** 1 segment_time_line={}".format(segment_time_line))

                    first_timestamp = 0
                    duration = 0
                    if end_index > -1:
                        first = True
                        last_s = None
                        next_timestamp = 0
                        for fragment in self.__streams[stream].fragments[start_index - 1:end_index]:

                            #print("&&&&&&&&&&&&&&&&&&&& fragment={}".format(fragment))

                            s = mpegdash.nodes.S()

                            if first is True:
                                first = False
                                s.t = fragment.timestamp
                                first_timestamp = fragment.timestamp

                            repeated = False
                            if last_s is not None:
                                if int(last_s.d) == int(fragment.duration * self.__streams[stream].time_scale) and next_timestamp == fragment.timestamp:
                                    repeated = True
                                    duration += int(fragment.duration * self.__streams[stream].time_scale)
                                    if last_s.r is not None:
                                        last_s.r = last_s.r + 1
                                    else:
                                        last_s.r = 1

                            if repeated is False:

                                if next_timestamp != fragment.timestamp:
                                    s.t = fragment.timestamp

                                s.d = int(fragment.duration * self.__streams[stream].time_scale)
                                duration += int(fragment.duration * self.__streams[stream].time_scale)

                                #print("&&&&&&&&&&&&&&&&&&&&&& ", s.t, s.d, s.r)
                                segment_time_line.Ss.append(s)

                                last_s = s

                            next_timestamp = fragment.timestamp + int(fragment.duration * self.__streams[stream].time_scale)

                            fragment_list.append(fragment)

                    if adaptation_set_id == self.__reference_adaptation_set_id:
                        #copy_segment_time_line = segment_time_line
                        reference_timescale = self.__streams[stream].time_scale
                        reference_start_time = first_timestamp
                        reference_duration = duration

                    #print("******************** 2 segment_time_line={}".format(segment_time_line))

        for eos_stream in self.__eos_streams:
            eos_period = self.__mpd.periods[0]
            for eos_adaptation_set in eos_period.adaptation_sets:

                eos_adaptation_set_id = adaptation_set.id
                eos_content_type = adaptation_set.content_type

                if eos_adaptation_set_id is None:
                    if eos_adaptation_set.content_components is not None:
                        eos_adaptation_set_id = eos_adaptation_set.content_components[0].id
                        eos_content_type = eos_adaptation_set.content_components[0].content_type

                if eos_content_type == 'text':#  and eos_adaptation_set_id == eos_stream:
                    eos_segment_template = eos_adaptation_set.segment_templates[0]

                    #eos_segment_template.segment_timelines = [copy_segment_time_line]

                    #print("reference_duration=", reference_duration)

                    if reference_duration > 0:
                        eos_segment_time_line = eos_segment_template.segment_timelines[0]
                        eos_segment_time_line.Ss = []
                        s = mpegdash.nodes.S()
                        s.t = int(reference_start_time * 1000 / reference_timescale)
                        s.d = 4000
                        r = reference_duration / reference_timescale
                        s.r = int(r / 4)
                        eos_segment_time_line.Ss.append(s)

        #delay = timedelta(seconds=self.__delay_seconds)
        #self.__mpd.publish_time = (datetime.utcnow() - delay).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.__mpd.publish_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # handle mpd time
        if self.__mpd_buffer_time_set is False:
            #self.__mpd.suggested_presentation_delay = 'PT' + str(self.__delay_seconds) + 'S'
            self.__mpd.suggested_presentation_delay = isodate.duration_isoformat(isodate.Duration(seconds=self.__delay_seconds))

            time_shift_buffer_depth = isodate.parse_duration(self.__mpd.time_shift_buffer_depth)
            added_duration = isodate.Duration(seconds=self.__delay_seconds)
            time_shift_buffer_depth += added_duration
            self.__mpd.time_shift_buffer_depth = isodate.duration_isoformat(time_shift_buffer_depth)

            max_segment_duration = isodate.parse_duration(self.__mpd.max_segment_duration)
            subtitles_segment_duration = isodate.Duration(seconds=4)
            if subtitles_segment_duration.tdelta > max_segment_duration:
                self.__mpd.max_segment_duration = isodate.duration_isoformat(subtitles_segment_duration)

            self.__mpd_buffer_time_set = True

        manifest = mpegdash.parser.MPEGDASHParser.get_as_doc(self.__mpd).toxml()

        #print("manifest=", manifest)

        self.__lock.release()

        return manifest, fragment_list

    ####################################################
    #  _read_audio_init
    ####################################################
    def _read_audio_init(self, audio_init_url: str) -> int:

        audio_init_request_wrapper = RequestWrapper(self.__session_id, 'DashLiveDelayHandler ' + audio_init_url)

        audio_init = None
        response = audio_init_request_wrapper.get(audio_init_url)
        if response is None:
            Utils.logger_.error(str(self.__session_id), "DashLiveDelayHandler::_read_audio_init error getting audio init from server")
            return 0

        audio_init = response.content

        init_decoder = DashInitDecoder(audio_init)
        audio_sampling_rate = init_decoder.read_audio_sampling_rate()

        return audio_sampling_rate

    ####################################################
    #  register_live_parser_listener
    ####################################################
    def register_live_parser_listener(self, listener: LiveDelayListener, param: Optional[str]) -> None:

        self.__listeners.append((listener, param))

    ####################################################
    #  __notify_listeners
    ####################################################
    def __notify_listeners(self, fragment: EosFragment) -> None:

        for listener, param in self.__listeners:
            listener.on_new_fragment(fragment, param)
