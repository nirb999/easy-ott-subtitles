import xml.etree.ElementTree as ET
from enum import Enum
import copy
import re
import json
import math
import threading
from typing import List, Optional, Dict, Any, Tuple

import mpegdash.parser
import m3u8
#import webvtt
from pycaption import WebVTTReader, WebVTTWriter, DFXPReader, DFXPWriter, CaptionSet, CaptionReadNoCaptions

import Utils as Utils
from GoogleCloudApi import GoogleCloudApi
from CommonTypes import EosNames, EosManifest, EosFragment, EosUrl, LiveDelayListener
from HlsLiveDelayHandler import HlsLiveDelayHandler
from DashLiveDelayHandler import DashLiveDelayHandler
from Languages import EosLanguage
from DashUtils import DashFragmentEncoder, DashFragmentParser


####################################################
#
#  OttProtocols
#
####################################################
class OttProtocols(Enum):
    HLS_PROTOCOL = 1
    DASH_PROTOCOL = 2

    @classmethod
    def to_string(self, ott_protocol):
        if ott_protocol == OttProtocols.HLS_PROTOCOL:
            return 'hls'
        if ott_protocol == OttProtocols.DASH_PROTOCOL:
            return 'dash'

    @classmethod
    def extension_string(self, ott_protocol):
        if ott_protocol == OttProtocols.HLS_PROTOCOL:
            return 'm3u8'
        if ott_protocol == OttProtocols.DASH_PROTOCOL:
            return 'mpd'


####################################################
#
#  OttHandler
#
####################################################
class OttHandler:
    _session_id: str
    _live: bool

    #################################
    # __init__
    #################################
    def __init__(self, session_id: str, live: bool) -> None:

        self._session_id = session_id
        self._live = live
        self._prev_dst_language = None
        self._prev_captions_map = {}

    #################################
    # close
    #################################
    def close(self) -> str:
        Utils.logger_.error(self._session_id, "OttHandler::close virtual function called")
        return ''

    #################################
    # get_extension
    #################################
    def get_extension(self) -> str:
        Utils.logger_.error(self._session_id, "OttHandler::get_extension virtual function called")
        return ''

    #################################
    # parse_manifest
    #################################
    def parse_manifest(self, original_manifest: str, variant_manifest_url: str, variants: List[int]) -> str:
        Utils.logger_.error(self._session_id, "OttHandler::parse_manifest virtual function called")
        return ''

    #################################
    # make_urls_absolute
    #################################
    def make_urls_absolute(self) -> None:
        Utils.logger_.error(self._session_id, "OttHandler::make_urls_absolute virtual function called")
        return

    #################################
    # redirect_urls
    #################################
    def redirect_urls(self, live_delay_seconds: int) -> None:
        Utils.logger_.error(self._session_id, "OttHandler::redirect_urls virtual function called")
        return

    #################################
    # add_subtitle_stream
    #################################
    def add_subtitle_stream(self, src_language: EosLanguage, dst_language: EosLanguage, default_lang: EosLanguage) -> None:
        Utils.logger_.error(self._session_id, "OttHandler::add_subtitle_stream virtual function called")
        return

    #################################
    # set_default_langauge
    #################################
    def set_default_langauge(self, default_lang: EosLanguage) -> None:
        Utils.logger_.error(self._session_id, "OttHandler::set_default_langauge virtual function called")
        return

    #################################
    # build_manifest
    #################################
    def build_manifest(self) -> str:
        Utils.logger_.error(self._session_id, "OttHandler::build_manifest virtual function called")
        return ''

    #################################
    # generate_subtitle_fragment
    #################################
    def generate_subtitle_fragment(self, start_time: Optional[float], end_time: Optional[float],
                                   subs: List[Dict[str, Any]],
                                   first_pts: Optional[int], first_start_time: Optional[int]) -> str:
        Utils.logger_.error(self._session_id, "OttHandler::generate_subtitle_fragment virtual function called")
        return ''

    #################################
    # register_live_parser_listener
    #################################
    def register_live_parser_listener(self, dst_language_code: str, listener: LiveDelayListener) -> None:
        Utils.logger_.error(self._session_id, "OttHandler::register_live_parser_listener virtual function called")
        return

    #################################
    # _translate_caption_set
    #################################
    def _translate_caption_set(self, caption_set: CaptionSet, next_caption_set: CaptionSet, src_language: EosLanguage, dst_language: EosLanguage):

        languages = caption_set.get_languages()
        captions = caption_set.get_captions(languages[0])

        next_captions = None
        if next_caption_set is not None:
            next_captions = next_caption_set.get_captions(languages[0])

        sentence = ''
        sentence_parts = []
        completed_sentences = []
        completed_sentences_parts = []

        print("\n **************************")

        prev_captions_map = copy.deepcopy(self._prev_captions_map)
        self._prev_captions_map = {}
        src_captions = copy.deepcopy(captions)
        src_next_captions = copy.deepcopy(next_captions)

        first_fragment_captions_set = set()
        
        caption_index = 0
        for caption in captions:  # for segment in parsed_webvtt:
            print("start: ", caption.start)
            print("end: ", caption.end)
            caption_nodes = caption.nodes
            caption_node_index = 0
            for caption_node in caption_nodes:  # for timestamp in parsed_webvtt[segment]:
                # print("type_: ", caption_node.type_)

                if caption_node.type_ == 1:
                    print("content: ", caption_node.content)

                    first_fragment_captions_set.add(caption_node.content)

                    if sentence != '':
                        sentence += ' '
                    sentence += caption_node.content
                    sentence_parts.append({'fragment_index': 1, 'caption_index': caption_index, 'caption_node_index': caption_node_index, 'words': len(caption_node.content.split())})

                if sentence.endswith('.') or sentence.endswith(',') or sentence.endswith(':') or sentence.endswith('?') or sentence.endswith('!') or sentence.endswith(';') or sentence.endswith('-'):
                    # print("sentence: ", sentence)
                    # print("sentence_parts: ", sentence_parts)

                    completed_sentences.append(sentence)
                    completed_sentences_parts.append(sentence_parts)

                    sentence = ''
                    sentence_parts = []

                caption_node_index += 1
            caption_index += 1

        # handle sentence at the end of the fragment which is not ended
        # TODO: merge with next fragment
        if sentence != '' and len(sentence_parts) > 0:
            print('%%%%%%%%%% open sentence')
            if next_captions is not None:
                stop = False
                caption_index = 0
                for caption in next_captions:
                    if stop is True:
                        print("1. stop!")
                        break

                    print("start: ", caption.start)
                    print("end: ", caption.end)
                    caption_nodes = caption.nodes
                    caption_node_index = 0
                    for caption_node in caption_nodes:  
                        if caption_node.type_ == 1:
                            print("content: ", caption_node.content)
                            if(caption_node.content in first_fragment_captions_set):
                                print("ignore this")
                                continue

                            if sentence != '':
                                sentence += ' '
                            sentence += caption_node.content
                            sentence_parts.append({'fragment_index': 2, 'caption_index': caption_index, 'caption_node_index': caption_node_index, 'words': len(caption_node.content.split())})

                        if sentence.endswith('.') or sentence.endswith(',') or sentence.endswith(':') or sentence.endswith('?') or sentence.endswith('!') or sentence.endswith(';') or sentence.endswith('-'):
                            # print("sentence: ", sentence)
                            # print("sentence_parts: ", sentence_parts)

                            completed_sentences.append(sentence)
                            completed_sentences_parts.append(sentence_parts)

                            sentence = ''
                            sentence_parts = []

                            stop = True
                            print("2. stop!")
                            break

                        caption_node_index += 1
                    caption_index += 1
                
        # handle sentence at the end of the fragment which is not ended
        # TODO: merge with next fragment
        if sentence != '' and len(sentence_parts) > 0:
                # no next fragment
                completed_sentences.append(sentence)
                completed_sentences_parts.append(sentence_parts)

        Utils.logger_.info(self._session_id, "OttHandler::_translate_caption_set using GCP translate {}->{}".format(src_language.code_bcp_47(), dst_language.code_bcp_47()))

        google_api = GoogleCloudApi()
        translations = google_api.translate(completed_sentences, src_language, dst_language)

        # print('len(completed_sentences): ', len(completed_sentences))
        # print('len(translations): ', len(translations))

        if len(completed_sentences) != len(translations):
            Utils.logger_.error(self._session_id, "OttHandler::_translate_caption_set number of completed sentences {} does not match number of translation result {}".format(len(completed_sentences), len(translations)))

        for translation, sentence, sentence_parts in zip(translations, completed_sentences, completed_sentences_parts):

            # print("translation: ", translation.translated_text)
            # print("sentence: ", sentence)
            # print("sentence_parts: ", sentence_parts)

            number_of_words = len(sentence.split())
            # print("number_of_words: ", number_of_words)

            translation_words = translation.translated_text.split()
            current_word_index = 0
            # number_of_parts = len(sentence_parts)

            for i, part in enumerate(sentence_parts, start=1):
                relative_number_of_words = math.ceil(part['words'] / number_of_words * len(translation_words))
                #print("part['words']={}, number_of_words={}, relative_number_of_words={}".format(part['words'], number_of_words, relative_number_of_words))

                if i < len(sentence_parts):
                    new_part_words = ' '.join(translation_words[current_word_index:current_word_index + relative_number_of_words])
                    current_word_index += relative_number_of_words
                else:
                    new_part_words = ' '.join(translation_words[current_word_index:])

                #print("current_word_index={}, new_part_words={}, translation_words={}".format(current_word_index, new_part_words, translation_words))

                if part['fragment_index'] == 1:
                    captions[part['caption_index']].nodes[part['caption_node_index']].content = new_part_words
                    print("translation start: ", captions[part['caption_index']].start)

                if part['fragment_index'] == 2:
                    next_captions[part['caption_index']].nodes[part['caption_node_index']].content = new_part_words
                    print("translation start: ", next_captions[part['caption_index']].start)

                print("translation: ", new_part_words)

        if self._prev_dst_language == dst_language:
            print("prev_captions_map: ", prev_captions_map)
            
            caption_index = 0
            for src_caption in src_captions:
                src_caption_nodes = src_caption.nodes
                src_caption_node_index = 0
                for src_caption_node in src_caption_nodes:
                    if src_caption_node.type_ == 1:
                        if src_caption_node.content in prev_captions_map:
                            print("!!!!!!!!!!!!!!!!! matched content: ", src_caption_node.content)
                            print("@@@@@@@ prev:",prev_captions_map[src_caption_node.content])
                            print("@@@@@@@ curr:", captions[caption_index].nodes[src_caption_node_index].content)
                            if captions[caption_index].nodes[src_caption_node_index].content != prev_captions_map[src_caption_node.content]:
                                print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ fixed")
                                captions[caption_index].nodes[src_caption_node_index].content = prev_captions_map[src_caption_node.content]

                        self._prev_captions_map[src_caption_node.content] = captions[caption_index].nodes[src_caption_node_index].content
                        print("insert 1: ", src_caption_node.content, ": ", captions[caption_index].nodes[src_caption_node_index].content)

                    src_caption_node_index += 1
                caption_index += 1

            caption_index = 0
            if src_next_captions is not None:
                for src_caption in src_next_captions:
                    src_caption_nodes = src_caption.nodes
                    src_caption_node_index = 0
                    for src_caption_node in src_caption_nodes:
                        if src_caption_node.type_ == 1:
                            if next_captions[caption_index].nodes[src_caption_node_index].content is None or next_captions[caption_index].nodes[src_caption_node_index].content == src_caption_node.content:
                                continue

                            self._prev_captions_map[src_caption_node.content] = next_captions[caption_index].nodes[src_caption_node_index].content
                            print("insert 2: ", src_caption_node.content, ": ", next_captions[caption_index].nodes[src_caption_node_index].content)
                            
                        src_caption_node_index += 1
                    caption_index += 1

        self._prev_dst_language = dst_language
        
        return caption_set


####################################################
#
#  HlsHandler
#
####################################################
class HlsHandler(OttHandler, LiveDelayListener):
    _version: str
    _copy_lines: List[str]
    _video_variants: List[EosManifest]
    _audio_variants: List[EosManifest]
    _text_variants: List[EosManifest]
    _reference_manifests: Dict[str, EosManifest]   # dst_lang -> reference manifest
    _live_streams: Dict[str, HlsLiveDelayHandler]   # manifest.url.base64_urlsafe -> hls delay handler

    #################################
    # __init__
    #################################
    def __init__(self, session_id: str, live: bool) -> None:

        OttHandler.__init__(self, session_id, live)

        self._version = ''
        self._copy_lines = []
        self._video_variants = []
        self._audio_variants = []
        self._text_variants = []
        self._reference_manifests = {}
        self._live_streams = {}

    #################################
    # close
    #################################
    def close(self) -> str:

        for stream in self._live_streams:
            self._live_streams[stream].close()

    #################################
    # get_extension
    #################################
    def get_extension(self) -> str:
        return '.m3u8'

    #################################
    # get_manifest_service_name
    #################################
    def get_manifest_service_name(self) -> str:
        return EosNames.eos_manifest_prefix

    #################################
    # get_live_service_name
    #################################
    def get_live_service_name(self) -> str:
        return EosNames.live_manifest_prefix

    #################################
    # get_fragment_service_name
    #################################
    def get_fragment_service_name(self) -> str:
        return EosNames.fragment_hls_prefix

    #################################
    # get_manifest_content_type
    #################################
    def get_manifest_content_type(self) -> str:
        return 'application/vnd.apple.mpegurl'

    #################################
    # register_live_parser_listener
    #################################
    def register_live_parser_listener(self, dst_language_code: str, listener: LiveDelayListener) -> None:
        if dst_language_code in self._reference_manifests:
            reference_manifest_base64_urlsafe = self._reference_manifests[dst_language_code].url.base64_urlsafe
            if reference_manifest_base64_urlsafe in self._live_streams:
                self._live_streams[reference_manifest_base64_urlsafe].register_live_parser_listener(listener, reference_manifest_base64_urlsafe)
            else:
                Utils.logger_.warning(self._session_id, "HlsHandler::register_live_parser_listener dst_language {} not found in live streams".format(dst_language_code))
        else:
            Utils.logger_.warning(self._session_id, "HlsHandler::register_live_parser_listener dst_language {} not found in reference manifests".format(dst_language_code))

    #################################
    # build_line
    #################################
    def build_line(self, _params: Dict[str, str], exclude_uri: bool = False) -> str:

        _str = ''
        first = True

        # make sure this is first
        if 'TYPE' in _params:
            _str += 'TYPE' + '=' + _params['TYPE']
            first = False

        for param in _params:
            if param == 'TYPE':
                continue

            if param == 'URI' and exclude_uri is True:
                continue

            #if param == 'PROGRAM-ID':
            #    continue

            if first is True:
                first = False
            else:
                _str += ','

            _str += param + '=' + _params[param]

        return _str

    #################################
    # parse_manifest
    #################################
    def parse_manifest(self, original_manifest: str, variant_manifest_url: str, variants: List[int]) -> None:

        parsed_manifest = m3u8.loads(original_manifest)

        if parsed_manifest.is_variant is False:
            print("error")
            return

        #for media in parsed_manifest.playlists:
        #    print("media:", media)

        next_is_video_url: bool = False
        variant_index: int = 0
        self.__copy_lines = []

        for line in original_manifest.splitlines():

            if next_is_video_url is True:
                self._video_variants[len(self._video_variants) - 1].url.set_url(line, variant_manifest_url)
                Utils.logger_.debug_color(self._session_id, "HlsHandler::parse_manifest video={}".format(self._video_variants[len(self._video_variants) - 1]))
                next_is_video_url = False

            elif '#EXT-X-VERSION:' in line:
                index = line.find(':') + 1
                self._version = line[index:].strip()
                Utils.logger_.debug_color(self._session_id, "HlsHandler::parse_manifest version={}".format(self._version))

            elif '#EXT-X-STREAM-INF:' in line:
                variant_index += 1
                new_video_variant = EosManifest()
                new_video_variant.manifest_params = Utils.parse_hls_params(line[len('#EXT-X-STREAM-INF:'):])
                if len(variants) == 0 or variant_index in variants:
                    self._video_variants.append(new_video_variant)
                    next_is_video_url = True

            elif '#EXT-X-MEDIA:' in line and 'TYPE=AUDIO' in line and 'URI="' in line:
                variant_index += 1
                new_audio_variant = EosManifest()
                new_audio_variant.manifest_params = Utils.parse_hls_params(line[len('#EXT-X-MEDIA:'):])
                new_audio_variant.url.set_url(new_audio_variant.manifest_params['URI'][1:-1], variant_manifest_url)
                if len(variants) == 0 or variant_index in variants:
                    self._audio_variants.append(new_audio_variant)
                    Utils.logger_.debug_color(self._session_id, "HlsHandler::parse_manifest audio={}".format(self._audio_variants[len(self._audio_variants) - 1]))

            elif '#EXT-X-MEDIA:' in line and 'TYPE=SUBTITLES' in line and 'URI="' in line:
                variant_index += 1
                new_text_variant = EosManifest()
                new_text_variant.manifest_params = Utils.parse_hls_params(line[len('#EXT-X-MEDIA:'):])
                if 'DEFAULT' in new_text_variant.manifest_params.keys():
                    new_text_variant.manifest_params['DEFAULT'] = 'NO'
                if 'AUTOSELECT' in new_text_variant.manifest_params.keys():
                    new_text_variant.manifest_params['AUTOSELECT'] = 'NO'
                new_text_variant.url.set_url(new_text_variant.manifest_params['URI'][1:-1], variant_manifest_url)
                if len(variants) == 0 or variant_index in variants:
                    self._text_variants.append(new_text_variant)
                    Utils.logger_.debug_color(self._session_id, "HlsHandler::parse_manifest text={}".format(self._text_variants[len(self._text_variants) - 1]))

            elif '#EXT-X-START:' in line or '#EXT-X-INDEPENDENT-SEGMENTS' in line or '#EXT-X-MEDIA:' in line:
                self._copy_lines.append(line)

    #################################
    # make_urls_absolute
    #################################
    def make_urls_absolute(self) -> None:

        for video in self._video_variants:
            video.manifest_params['URI'] = video.url.absolute_url

        for audio in self._audio_variants:
            audio.manifest_params['URI'] = '"' + audio.url.absolute_url + '"'

        for text in self._text_variants:
            text.manifest_params['URI'] = '"' + text.url.absolute_url + '"'

    #################################
    # redirect_urls
    #################################
    def redirect_urls(self, live_delay_seconds: int) -> None:

        for video in self._video_variants:
            video.manifest_params['URI'] = self.get_live_service_name() + "/" + video.url.base64_urlsafe + "/index.m3u8"
            if self._live is True:
                self._live_streams[video.url.base64_urlsafe] = HlsLiveDelayHandler(self._session_id, video.url.base64_urlsafe, live_delay_seconds)
                self._live_streams[video.url.base64_urlsafe].start()

        for audio in self._audio_variants:
            audio.manifest_params['URI'] = self.get_live_service_name() + "/" + audio.url.base64_urlsafe + "/index.m3u8"
            if self._live is True:
                self._live_streams[audio.url.base64_urlsafe] = HlsLiveDelayHandler(self._session_id, audio.url.base64_urlsafe, live_delay_seconds)
                self._live_streams[audio.url.base64_urlsafe].start()

        for text in self._text_variants:
            text.manifest_params['URI'] = self.get_live_service_name() + "/" + text.url.base64_urlsafe + "/index.m3u8"
            if self._live is True:
                self._live_streams[text.url.base64_urlsafe] = HlsLiveDelayHandler(self._session_id, text.url.base64_urlsafe, live_delay_seconds)
                self._live_streams[text.url.base64_urlsafe].start()

    #################################
    # add_subtitle_stream
    #################################
    def add_subtitle_stream(self, src_language: EosLanguage, dst_language: EosLanguage, default_lang: EosLanguage) -> None:

        # find audio or video+audio with src_langauge
        matched_audio: EosManifest = None
        matched_video: EosManifest = None

        for audio in self._audio_variants:
            if 'LANGUAGE' in audio.manifest_params:
                if audio.manifest_params['LANGUAGE'][1:-1] in src_language.codes():
                    matched_audio = audio
                    Utils.logger_.info(self._session_id, "HlsHandler::add_subtitle_stream found matching audio variant")
                    break
            else:
                Utils.logger_.warning(self._session_id, "HlsHandler::add_subtitle_stream audio variant does not have LANGUAGE attribute")
                matched_audio = audio
                Utils.logger_.info(self._session_id, "HlsHandler::add_subtitle_stream found matching audio variant")
                #break

        if matched_audio is None:
            Utils.logger_.warning(self._session_id, "HlsHandler::add_subtitle_stream can't find audio stream for language {}".format(src_language.code_bcp_47()))

            # audio variant not found, look for video which contians audio
            # take the lowest video bitrate possible
            video_variants = {}
            for video in self._video_variants:
                if 'CODECS' in video.manifest_params:
                    if 'mp4a' in video.manifest_params['CODECS']:
                        if int(video.manifest_params['BANDWIDTH']) not in video_variants:
                            video_variants[int(video.manifest_params['BANDWIDTH'])] = video

            if len(video_variants) > 0:
                # peek the lowest bitrate
                min_bitrate = min(video_variants)
                matched_video = video_variants[min_bitrate]

        if matched_video is None:
            Utils.logger_.warning(self._session_id, "HlsHandler::add_subtitle_stream can't find video stream with codec info {}".format('mp4a'))

            # still not found, try the lowest video stream
            video_variants = {}
            for video in self._video_variants:
                if int(video.manifest_params['BANDWIDTH']) not in video_variants:
                    video_variants[int(video.manifest_params['BANDWIDTH'])] = video

            if len(video_variants) > 0:
                # peek the lowest bitrate
                min_bitrate = min(video_variants)
                matched_video = video_variants[min_bitrate]

        if matched_video is None:
            Utils.logger_.warning(self._session_id, "HlsHandler::add_subtitle_stream can't find video stream for language {}".format(src_language.code_bcp_47()))
            return

        # try to clone a text variant, if there is one
        # if len(self._text_variants) > 0:

        # add 'SUBTITLES' to all video variants
        # if there is alradey one, use it. if not, add one.
        group_id = ''

        for video in self._video_variants:

            if 'SUBTITLES' in video.manifest_params:

                if group_id == '':
                    group_id = video.manifest_params['SUBTITLES']
                else:
                    if group_id != video.manifest_params['SUBTITLES']:
                        Utils.logger_.warning(self._session_id, "HlsHandler::add_subtitle_stream multiple SUBTITLES values {}, {}".format(group_id, video.manifest_params['SUBTITLES']))

        if group_id == '':
            group_id = '"' + 'WebVTT' + '"'
            for video in self._video_variants:
                video.manifest_params['SUBTITLES'] = group_id

        #for video in self._video_variants:
        #    video.manifest_params['CLOSED-CAPTIONS'] = '"NONE"'

        new_text = EosManifest()
        new_text.eos = True
        new_text.manifest_params['TYPE'] = 'SUBTITLES'
        new_text.manifest_params['GROUP-ID'] = group_id
        new_text.manifest_params['LANGUAGE'] = '"' + dst_language.code_639_1() + '"'
        new_text.manifest_params['NAME'] = '"' + dst_language.name() + ' (Eos)"'
        new_text.manifest_params['AUTOSELECT'] = 'NO'
        new_text.manifest_params['FORCED'] = 'NO'
        new_text.manifest_params['DEFAULT'] = 'NO'

        Utils.logger_.dump(self._session_id, "default_lang={}, dst_language={}".format(default_lang, dst_language))
        if default_lang == dst_language:
            new_text.manifest_params['AUTOSELECT'] = 'YES'
            new_text.manifest_params['DEFAULT'] = 'YES'

        if matched_audio is not None:
            new_text.manifest_params['URI'] = '\"{}/{}/{}/{}\"'.format(self.get_manifest_service_name(), dst_language.code_bcp_47(), matched_audio.url.base64_urlsafe, "index.m3u8")

            self._reference_manifests[dst_language.code_bcp_47()] = matched_audio

            if self._live is True:
                self._live_streams[matched_audio.url.base64_urlsafe].register_live_parser_listener(self, matched_audio.url.base64_urlsafe)

            Utils.logger_.info_y(self._session_id, "HlsHandler::add_subtitle_stream matched_audio={}".format(matched_audio))

        elif matched_video is not None:
            new_text.manifest_params['URI'] = '\"{}/{}/{}/{}\"'.format(self.get_manifest_service_name(), dst_language.code_bcp_47(), matched_video.url.base64_urlsafe, "index.m3u8")

            self._reference_manifests[dst_language.code_bcp_47()] = matched_video

            if self._live is True:
                self._live_streams[matched_video.url.base64_urlsafe].register_live_parser_listener(self, matched_video.url.base64_urlsafe)

            Utils.logger_.info_y(self._session_id, "HlsHandler::add_subtitle_stream matched_video={}".format(matched_video))

        self._text_variants.append(new_text)

    #################################
    # clone_subtitle_stream
    #################################
    def clone_subtitle_stream(self, src_language: EosLanguage, dst_language: EosLanguage, default_language: EosLanguage) -> None:

        matched_text: EosManifest = None

        for text in self._text_variants:
            if text.manifest_params['LANGUAGE'][1:-1] in src_language.codes():
                matched_text = text
                break

        if matched_text is None:
            Utils.logger_.error(self._session_id, "HlsHandler::clone_subtitle_stream can't find text stream for language {}".format(src_language.code_bcp_47()))
            return

        new_text = EosManifest()
        new_text.eos = True
        new_text.manifest_params = copy.deepcopy(matched_text.manifest_params)

        new_text.manifest_params['LANGUAGE'] = '"' + dst_language.code_639_2() + '"'
        new_text.manifest_params['NAME'] = '"' + dst_language.name() + ' (Eos)"'
        new_text.manifest_params['AUTOSELECT'] = 'YES'

        # set default language if needed
        if default_language == dst_language:
            new_text.manifest_params['DEFAULT'] = 'YES'
            if 'DEFAULT' in matched_text.manifest_params:
                matched_text.manifest_params['DEFAULT'] = 'NO'

        new_text.manifest_params['URI'] = '\"{}/{}/{}/{}\"'.format(self.get_manifest_service_name(), dst_language.code_bcp_47(), matched_text.url.base64_urlsafe, "index.m3u8")

        self._text_variants.append(new_text)

        self._reference_manifests[dst_language.code_bcp_47()] = matched_text

        Utils.logger_.info_y(self._session_id, "HlsHandler::clone_subtitle_stream matched_text={}".format(matched_text))

    #################################
    # set_default_langauge
    #################################
    def set_default_langauge(self, default_lang: EosLanguage) -> None:

        if default_lang is not None:
            for text_veriant in self._text_variants:
                if text_veriant.eos is True:
                    if 'LANGUAGE' in text_veriant.manifest_params.keys():
                        if 'LANGUAGE' in text_veriant.manifest_params and text_veriant.manifest_params['LANGUAGE'][1:-1] == default_lang.code_639_2():
                            text_veriant.manifest_params['DEFAULT'] = 'YES'
                        else:
                            text_veriant.manifest_params['DEFAULT'] = 'NO'

    #################################
    # build_manifest
    #################################
    def build_manifest(self) -> str:

        modified_manifest = ''

        modified_manifest += '#EXTM3U'
        modified_manifest += '\n'

        #if self._version != '':
        modified_manifest += '#EXT-X-VERSION:' + '5'  # self._version
        modified_manifest += '\n'

        for line in self._copy_lines:
            modified_manifest += line
            modified_manifest += '\n'

        for text in self._text_variants:
            # print("text: ", text)
            modified_manifest += '#EXT-X-MEDIA:'
            modified_manifest += self.build_line(text.manifest_params)
            modified_manifest += '\n'

        for audio in self._audio_variants:
            # print("audio: ", audio)
            modified_manifest += '#EXT-X-MEDIA:'
            modified_manifest += self.build_line(audio.manifest_params)
            modified_manifest += '\n'

        for video in self._video_variants:
            # print("video: ", video)
            modified_manifest += '#EXT-X-STREAM-INF:'
            modified_manifest += self.build_line(video.manifest_params, exclude_uri=True)
            modified_manifest += '\n'
            modified_manifest += video.manifest_params['URI']
            modified_manifest += '\n'

        return modified_manifest

    #################################
    # clone_reference_manifest
    #################################
    def clone_reference_manifest(self, reference_manifest: str, dst_language: str, reference_manifest_url: str) -> str:

        parsed_manifest = m3u8.loads(reference_manifest)

        start_time: float = 0.0

        for segment in parsed_manifest.segments:
            #print("segment:", segment)

            fragment = EosFragment()
            fragment.url.set_url(segment.uri, reference_manifest_url)
            fragment.duration = segment.duration
            fragment.start_time = start_time
            start_time += segment.duration

            if segment.key is not None:
                new_url = EosUrl()
                new_url.set_url(segment.key.uri, reference_manifest_url)
                fragment.encryption_uri = new_url.absolute_url
                fragment.encryption_iv = segment.key.iv
                fragment.encryption_method = segment.key.method

            fragment.discontinuity = segment.discontinuity

            segment.uri = '{}/{}'.format(self.get_fragment_service_name(), fragment.url.base64_urlsafe)

            if self._live is False:
                self._reference_manifests[dst_language].fragments.append(fragment)

        cloned_manifest = parsed_manifest.dumps()

        return cloned_manifest

    #################################
    # build_live_subtitle_manifest
    #################################
    def build_live_subtitle_manifest(self, fragment_list: List[EosFragment]) -> str:

        manifest = ''

        manifest += '#EXTM3U\n'
        manifest += '#EXT-X-VERSION:5\n'
        manifest += '#EXT-X-TARGETDURATION:15\n'

        if len(fragment_list) > 0:
            manifest += '#EXT-X-MEDIA-SEQUENCE:' + str(fragment_list[0].media_sequence) + '\n' 

        for fragment in fragment_list:

            if fragment.discontinuity == True:
                manifest += '#EXT-X-DISCONTINUITY'
                manifest += '\n'

            manifest += '#EXTINF:' + "{:.2f}".format(fragment.duration) + ',\n'
            manifest += '{}/{}'.format(self.get_fragment_service_name(), fragment.url.base64_urlsafe) + '\n'

        return manifest

    #################################
    # on_new_fragment
    # derived from LiveDelayListener
    #################################
    def on_new_fragment(self, fragment: EosFragment, param: str) -> None:
        # Utils.logger_.debug(self._session_id, "HlsHandler::on_new_fragment fragment={}".format(fragment))
        for manifest in self._reference_manifests:
            if self._reference_manifests[manifest].url.base64_urlsafe == param:
                self._reference_manifests[manifest].fragments.append(fragment)
                break

    #################################
    # get_live_manifest
    #################################
    def get_live_manifest(self, live_origin_manifest_url_base64: str) -> Tuple[str, List[EosFragment]]:

        # print(self._live_streams)

        live_manifest, fragemnt_list = self._live_streams[live_origin_manifest_url_base64].delay()

        return live_manifest, fragemnt_list

    #################################
    # get_reference_manifest_url
    #################################
    def get_reference_manifest_url(self, dst_lang: EosLanguage) -> str:
        return self._reference_manifests[dst_lang.code_bcp_47()].url.base64_urlsafe

    #################################
    # get_fragments_list
    #################################
    def get_fragments_list(self, dst_lang: EosLanguage) -> List[EosFragment]:
        return self._reference_manifests[dst_lang.code_bcp_47()].fragments

    #################################
    # get_start_stop_times
    #################################
    def get_start_stop_times(self, dst_lang: str, fragment_base64_uri: str):  # -> Optional[float], Optional[float]

        for fragment in self._reference_manifests[dst_lang].fragments:

            # print("get_start_stop_times fragment.url.base64_urlsafe={}, uri={}".format(fragment.url.base64_urlsafe, fragment_base64_uri))

            if fragment.url.base64_urlsafe == fragment_base64_uri:
                start_time = fragment.start_time
                end_time = fragment.start_time + fragment.duration
                return start_time, end_time

        return None, None

    #################################
    # generate_subtitle_fragment
    #################################
    def generate_subtitle_fragment(self, start_time: Optional[float], end_time: Optional[float],
                                   subs: List[Dict[str, Any]],
                                   first_pts: Optional[int], first_start_time: Optional[int]) -> str:

        # TODO: use webvtt-py and pycaption

        webvtt_fragment = ''
        webvtt_fragment += 'WEBVTT\n'

        if self._live is True:

            webvtt_fragment += 'X-TIMESTAMP-MAP=MPEGTS:' + str(first_pts) + ',LOCAL:' + Utils.seconds_to_webvtt_time(first_start_time) + '\n'
            # webvtt_fragment += 'X-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n'

        if start_time is not None and end_time is not None:
            for sub in subs:
                if (start_time >= sub['start'] and start_time < sub['end']) or \
                   (end_time > sub['start'] and end_time <= sub['end']) or \
                   (start_time <= sub['start'] and end_time >= sub['end']):

                    webvtt_fragment += ('\n' + Utils.seconds_to_webvtt_time(sub['start']) + ' --> ' + Utils.seconds_to_webvtt_time(sub['end']) + '\n')
                    webvtt_fragment += sub['text'] + '\n'

        webvtt_fragment += '\n'

        return webvtt_fragment

    #################################
    # get_next_fragment_uri
    #################################
    def get_next_fragment_uri(self, fragment_uri: str) -> Optional[str]:

        # print("fragment_uri: ", fragment_uri)

        next_fragment_uri = None
        use_next_fragment = False
        offset = -1

        for fragment in self._fragments:

            # print("fragment['URI']: ", fragment['URI'])

            if use_next_fragment is True:
                next_fragment_uri = fragment_uri[:offset] + fragment['URI']
                break

            offset = fragment_uri.find(fragment['URI'])
            if offset != -1:
                use_next_fragment = True

        return next_fragment_uri

    #################################
    # translate_subtitle_fragment
    #################################
    def translate_subtitle_fragment(self, src_fragment, src_next_fragment, src_language: EosLanguage, dst_language: EosLanguage):

        # print("type(src_fragment): ", type(src_fragment))
        # print("src_fragment: ", src_fragment.decode('utf-8'))

        # TODO: pycaption parse X-TIMESTAMP-MAP

        try:
            caption_set = WebVTTReader().read(src_fragment.decode('utf-8'))
        except CaptionReadNoCaptions:
            Utils.logger_.error(self._session_id, "DashHandler::translate_subtitle_fragment error CaptionReadNoCaptions")
            return src_fragment

        caption_set = self._translate_caption_set(caption_set, src_language, dst_language)

        modified_fragment = WebVTTWriter().write(caption_set)
        # print("modified_fragment: ", modified_fragment)

        return modified_fragment.encode('utf-8')


####################################################
#
#  DashHandler
#
####################################################
class DashHandler(OttHandler):
    __variant_manifest_url: str
    __mpd: Optional[Any]
    #__mpd: Optional[ET.Element]
    __mpd_namespace: Optional[str]
    __adaptation_sets: Dict[int, Dict[str, str]]  # [adaptation_set_id, [parameter, value]]
    __next_adaptation_set_id: int
    _reference_manifests: Dict[str, EosManifest]   # dst_lang -> reference manifest
    _live_stream: Optional[DashLiveDelayHandler]
    _reference_audio_adaptation_set_id_: Optional[str]

    #################################
    # __init__
    #################################
    def __init__(self, session_id: str, live: bool) -> None:

        OttHandler.__init__(self, session_id, live)

        self.__variant_manifest_url = ''
        self.__mpd = None
        self.__mpd_namespace = None
        self.__adaptation_sets = {}
        self.__next_adaptation_set_id = 0
        self._reference_manifests = {}
        self._live_stream = None
        self._reference_audio_adaptation_set_id_ = None

    #################################
    # get_extension
    #################################
    def get_extension(self) -> str:
        return '.mpd'

    #################################
    # get_manifest_content_type
    #################################
    def get_manifest_content_type(self) -> str:
        return 'xml'

    #################################
    # parse_manifest
    #################################
    def parse_manifest(self, original_manifest: str, variant_manifest_url: str, variants: List[int]) -> None:

        self.__variant_manifest_url = variant_manifest_url

        self.__mpd = mpegdash.parser.MPEGDASHParser.parse(original_manifest)

        self._version = ''

        period = self.__mpd.periods[0]

        for adaptation_set in period.adaptation_sets:

            adaptation_set_id = adaptation_set.id
            content_type = adaptation_set.content_type

            if adaptation_set_id is None:
                if adaptation_set.content_components is not None:
                    adaptation_set_id = adaptation_set.content_components[0].id
                    content_type = adaptation_set.content_components[0].content_type

            self.__adaptation_sets[adaptation_set_id] = {}
            self.__adaptation_sets[adaptation_set_id]['content_type'] = content_type

            if content_type == 'video':
                pass
            elif content_type == 'audio':
                self.__adaptation_sets[adaptation_set_id]['language'] = adaptation_set.lang
            elif content_type == 'text':
                self.__adaptation_sets[adaptation_set_id]['language'] = adaptation_set.lang

            segment_template = adaptation_set.segment_templates[0]

            self.__adaptation_sets[adaptation_set_id]['media'] = segment_template.media
            self.__adaptation_sets[adaptation_set_id]['initialization'] = segment_template.initialization

            timescale = segment_template.timescale

            total_fragments = 0
            total_duration = 0

            segment_time_line = segment_template.segment_timelines[0]
            for s in segment_time_line.Ss:

                if s.r is not None:
                    r = s.r + 1
                    total_fragments += r
                else:
                    r = 1
                    total_fragments += r

                d = float(s.d)
                total_duration += (r * d / timescale)

            #print('total_fragments:', total_fragments)
            #print('total_duration:', total_duration)

            self.__adaptation_sets[adaptation_set_id]['total_fragments'] = total_fragments
            self.__adaptation_sets[adaptation_set_id]['total_duration'] = total_duration

            if content_type == 'video':
                pass
            elif content_type == 'audio':
                pass
            elif content_type == 'text':
                pass

            Utils.logger_.info_y(self._session_id, "DashHandler::parse_manifest adaptation_set_id {}: {}".format(adaptation_set_id, self.__adaptation_sets[adaptation_set_id]))

        sorted_adaptation_sets = sorted(self.__adaptation_sets)
        self.__next_adaptation_set_id = sorted_adaptation_sets[-1] + 1

    #################################
    # make_urls_absolute
    #################################
    def make_urls_absolute(self) -> None:

        # this is called only for vod

        #TODO: handle BaseUrl

        period = self.__mpd.periods[0]
        for adaptation_set in period.adaptation_sets:
            segment_template = adaptation_set.segment_templates[0]

            new_url = EosUrl()
            new_url.set_url(segment_template.media, self.__variant_manifest_url)
            segment_template.media = new_url.absolute_url

            new_url = EosUrl()
            new_url.set_url(segment_template.initialization, self.__variant_manifest_url)
            segment_template.initialization = new_url.absolute_url

    #################################
    # register_live_parser_listener
    #################################
    def register_live_parser_listener(self, dst_language_code: str, listener: LiveDelayListener) -> None:
        self._live_stream.register_live_parser_listener(listener, None)

    #################################
    # redirect_urls
    #################################
    def redirect_urls(self, live_delay_seconds: int) -> None:

        # this is called only for live
        if self._live_stream is None:
            ready = threading.Event()
            self._live_stream = DashLiveDelayHandler(self._session_id, self.__variant_manifest_url, live_delay_seconds, ready)
            self._live_stream.start()
            ready.wait()

    #################################
    # add_subtitle_stream
    #################################
    def add_subtitle_stream(self, src_language: EosLanguage, dst_language: EosLanguage, default_lang: EosLanguage) -> None:

        # find audio with src_langauge
        matched_audio_id = -1
        for adaptation_set_id in self.__adaptation_sets:
            if self.__adaptation_sets[adaptation_set_id]['content_type'] == 'audio':
                if self.__adaptation_sets[adaptation_set_id]['language'] in src_language.codes():
                    matched_audio_id = adaptation_set_id
                    break

        if matched_audio_id == -1:
            Utils.logger_.warning(self._session_id, "DashHandler::add_subtitle_stream can't find audio stream for language {}".format(src_language.code_bcp_47()))
            return

        self._reference_audio_adaptation_set_id_ = matched_audio_id
        Utils.logger_.info_y(self._session_id, "DashHandler::add_subtitle_stream matched_audio_id={}".format(matched_audio_id))
        #if self._live is True:
        #    self._live_stream.set_reference_stream(self._reference_audio_adaptation_set_id_, self.__next_adaptation_set_id, dst_language, default_lang)
        #self.__next_adaptation_set_id += 1
        #return

        # create new adaptation set for the dst_language
        adaptation_set = mpegdash.nodes.AdaptationSet()
        adaptation_set.id = self.__next_adaptation_set_id
        self.__next_adaptation_set_id += 1
        adaptation_set.group = 8
        adaptation_set.bitstream_switching = True
        adaptation_set.segment_alignment = True
        adaptation_set.content_type = "text"
        #adaptation_set.codecs = "stpp"
        adaptation_set.mime_type = "application/mp4"
        #adaptation_set.start_with_sap = 1
        adaptation_set.lang = dst_language.code_639_2()

        role = mpegdash.nodes.Descriptor()
        role.scheme_id_uri = "urn:mpeg:dash:role:2011"
        role.value = "subtitle"
        adaptation_set.roles = [role]

        representation = mpegdash.nodes.Representation()
        representation.id = "dxFknw.." + dst_language.code_639_1()
        representation.bandwidth = 100
        representation.codecs = "stpp"
        adaptation_set.representations = [representation]

        segment_template = mpegdash.nodes.SegmentTemplate()
        segment_template.timescale = 10000000
        segment_template.presentation_time_offset = 0
        segment_template.media = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio_id]['media'])
        segment_template.initialization = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio_id]['initialization'])

        segment_timeline = mpegdash.nodes.SegmentTimeline()
        s = mpegdash.nodes.S()
        s.d = 40000000
        s.r = int(self.__adaptation_sets[matched_audio_id]['total_duration'] / 4)
        segment_timeline.Ss = [s]

        segment_template.segment_timelines = [segment_timeline]
        adaptation_set.segment_templates = [segment_template]

        self.__mpd.periods[0].adaptation_sets.append(adaptation_set)


        ref_manifest = EosManifest()

        period = self.__mpd.periods[0]
        for adaptation_set in period.adaptation_sets:
            adaptation_set_id = adaptation_set.id
            if int(adaptation_set_id) == matched_audio_id:

                #new_adaptation_set = copy.deepcopy(adaptation_set)

                #new_adaptation_set.id = self.__next_adaptation_set_id
                #self.__next_adaptation_set_id += 1
                #new_adaptation_set.lang = dst_language.code_639_2()
                #new_adaptation_set.group = 8
                #new_adaptation_set.bitstream_switching = True
                #new_adaptation_set.segment_alignment = True
                #new_adaptation_set.content_type = "text"
                #new_adaptation_set.mime_type = "application/mp4"
                #new_adaptation_set.audio_channel_configurations = None

                #role = mpegdash.nodes.Descriptor()
                #role.scheme_id_uri = "urn:mpeg:dash:role:2011"
                #role.value = "subtitle"
                #new_adaptation_set.roles = [role]

                segment_template = adaptation_set.segment_templates[0]
                media = segment_template.media
                timescale = segment_template.timescale
                #segment_template.media = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio_id]['media'])
                #segment_template.initialization = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio_id]['initialization'])

                representation = adaptation_set.representations[0]
                #representation.codecs = "stpp"
                representation_id = representation.id
                #representation.id = "dxFknw.." + dst_language.code_639_1()
                bandwidth = representation.bandwidth
                #representation.bandwidth = 100
                audio_sampling_rate = representation.audio_sampling_rate
                #representation.audio_sampling_rate = None

                #self.__mpd.periods[0].adaptation_sets.append(new_adaptation_set)


                segment_timeline = segment_template.segment_timelines[0]
                current_timestamp: int = 0
                for s in segment_timeline.Ss:
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
                        new_media = media
                        new_media = new_media.replace('$Bandwidth$', str(bandwidth))
                        new_media = new_media.replace('$Time$', str(current_timestamp))
                        new_media = new_media.replace('$RepresentationID$', representation_id)

                        new_fragment = EosFragment()
                        new_fragment.url.set_url(new_media, self.__variant_manifest_url)
                        new_fragment.sampling_rate = int(audio_sampling_rate)
                        new_fragment.timestamp = current_timestamp
                        new_fragment.duration = (duration / timescale)
                        #new_fragment.start_time = self.__streams[stream_key].current_time
                        #new_fragment.first_read = self.__first_manifest_read

                        #print("new_fragment: ", new_fragment)
                        if self._live is False:
                            ref_manifest.fragments.append(new_fragment)

                        current_timestamp += int(duration)
                break

        self._reference_manifests[dst_language.code_bcp_47()] = ref_manifest

        return
        adaptation_set = mpegdash.nodes.AdaptationSet()
        adaptation_set.id = str(self.__next_adaptation_set_id)
        self.__next_adaptation_set_id += 1
        adaptation_set.group = 8
        adaptation_set.bitstream_switching = True
        adaptation_set.segment_alignment = True
        adaptation_set.content_type = "text"
        adaptation_set.lang = dst_language.code_639_1()

        #adaptation_set = ET.Element('AdaptationSet')
        #adaptation_set.set('id', str(self.get_next_adaptation_set_id()))
        #adaptation_set.set('group', "8")
        #adaptation_set.set('bitstreamSwitching', "true")
        #adaptation_set.set('segmentAlignment', "true")
        #adaptation_set.set('contentType', "text")
        #adaptation_set.set('mimeType', "application/mp4")
        #adaptation_set.set('lang', dst_language.code_639_1())

        #role = ET.SubElement(adaptation_set, 'Role')
        #role.set('schemeIdUri', "urn:mpeg:dash:role:2011")
        #role.set('value', "subtitle")

        segment_template = mpegdash.nodes.SegmentTemplate()
        segment_template.timescale = 10000000
        #segment_template.media = "{}/Fragments(en_0=$Time$)".format(EosNames.fragment_dash_prefix)
        #segment_template.initialization = "{}/Fragments(en_0=Init)".format(EosNames.fragment_dash_prefix)
        segment_template.media = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio]['media'])
        segment_template.initialization = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[matched_audio]['initialization'])


        segment_timeline = mpegdash.nodes.SegmentTimeline()

        #segment_template = ET.SubElement(adaptation_set, 'SegmentTemplate')
        #segment_template.set('timescale', "10000000")
        #segment_template.set('media', "{}/Fragments(en_0=$Time$)".format(EosNames.fragment_dash_prefix))
        #segment_template.set('initialization', "{}/Fragments(en_0=Init)".format(EosNames.fragment_dash_prefix))
        #segment_template.set('timescale', "10000000")

        #segment_timeline = ET.SubElement(segment_template, 'SegmentTimeline')

        s = mpegdash.nodes.S()
        s.d = 40000000
        s.r = 764

        #s = ET.SubElement(segment_timeline, 'S')
        #s.set('d', "40000000")
        #s.set('r', "764")

        #representation = ET.SubElement(adaptation_set, 'Representation')
        #representation.set('id', "dxFknw..")
        #representation.set('bandwidth', "100")
        #representation.set('codecs', "stpp")

        segment_timeline.write(s)
        segment_template.segment_timelines = []
        segment_template.segment_timelines.append(segment_timeline)
        adaptation_set.segment_templates = []
        adaptation_set.segment_templates.append(segment_template)

        self.__mpd.periods[0].adaptation_sets.append(adaptation_set)

        # self.__mpd[0].append(adaptation_set)

    #################################
    # clone_subtitle_stream
    #################################
    def clone_subtitle_stream(self, src_language: EosLanguage, dst_language: EosLanguage, default_language: EosLanguage) -> None:

        src_id = -1

        for adaptation_set in self.__adaptation_sets:
            if self.__adaptation_sets[adaptation_set]['content_type'] == 'text':
                if self.__adaptation_sets[adaptation_set]['language'] in src_language.codes():
                    src_id = adaptation_set
                    break

        if src_id == -1:
            Utils.logger_.error(self._session_id, "DashHandler::clone_subtitle_stream can't find text stream for language {}".format(src_language))
            return

        period = self.__mpd.periods[0]
        for adaptation_set in period.adaptation_sets:
            adaptation_set_id = adaptation_set.id
            if int(adaptation_set_id) == src_id:

                new_adaptation_set = copy.deepcopy(adaptation_set)

                new_adaptation_set.id = self.__next_adaptation_set_id
                self.__next_adaptation_set_id += 1
                new_adaptation_set.lang = dst_language.code_639_2()

                segment_template = new_adaptation_set.segment_templates[0]
                segment_template.media = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[src_id]['media'])
                segment_template.initialization = "{}/{}/{}/{}".format(EosNames.eos_manifest_prefix, dst_language.code_bcp_47(), EosNames.fragment_dash_prefix, self.__adaptation_sets[src_id]['initialization'])

                representation = new_adaptation_set.representations[0]
                representation.id += dst_language.code_639_1()

                self.__mpd.periods[0].adaptation_sets.append(new_adaptation_set)
                break

    #################################
    # build_manifest
    #################################
    def build_manifest(self) -> str:

        if self._live is False:

            modified_manifest = mpegdash.parser.MPEGDASHParser.toprettyxml(self.__mpd)

            #modified_manifest = ET.tostring(self.__mpd, encoding="unicode")
            #modified_manifest = '<?xml version="1.0" encoding="UTF-8"?>\n' + modified_manifest

            return modified_manifest

        else:

            live_manifest, fragemnt_list = self._live_stream.delay()
            return live_manifest

    #################################
    # generate_init_fragment
    #################################
    def generate_init_fragment(self) -> bytes:

        dash_encoder = DashFragmentEncoder()
        init_fragment = dash_encoder.build_subtitles_initialization(10000000)
        return init_fragment

    #################################
    # pack_subtitle_fragment
    #################################
    def pack_subtitle_fragment(self, start_time, end_time, subtitle_ttml) -> bytes:

        dash_encoder = DashFragmentEncoder()
        subtitle_fragment = dash_encoder.build_subtitles_fragment(10000000, start_time, end_time, subtitle_ttml)
        return subtitle_fragment

    #################################
    # generate_subtitle_fragment
    #################################
    def generate_subtitle_fragment(self, start_time: Optional[float], end_time: Optional[float],
                                   subs: List[Dict[str, Any]]) -> str:

        # TODO: use pycaption

        ttml_fragment = ''

        ttml_fragment += "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
        ttml_fragment += "<tt xml:lang=\"\" xmlns=\"http://www.w3.org/ns/ttml\" xmlns:tt=\"http://www.w3.org/ns/ttml\" xmlns:tts=\"http://www.w3.org/ns/ttml#styling\">\r\n"

        # head
        ttml_fragment += "  <head>\r\n"

        # styling
        ttml_fragment += "    <styling>\r\n"
        ttml_fragment += "      <style xml:id=\"s0\" tts:backgroundColor=\"rgba(0,0,0,192)\" tts:color=\"rgba(255,255,255,255)\" tts:fontSize=\"0.80c\" tts:fontFamily=\"proportionalSansSerif\" tts:textAlign=\"center\" tts:displayAlign=\"center\"/>\r\n"
        ttml_fragment += "    </styling>\r\n"

        # layout - single region
        ttml_fragment += "    <layout>\r\n"
        ttml_fragment += "      <region xml:id=\"r0\" tts:origin=\"2.84% 84.00%\" tts:extent=\"94.32% 16%\" />\r\n"
        ttml_fragment += "    </layout>\r\n"

        ttml_fragment += "  </head>\r\n"

        # body
        ttml_fragment += "  <body>\r\n"
        ttml_fragment += "    <div>\r\n"

        if start_time is not None and end_time is not None:

            presentation_time_offset_sec = 0#self._live_stream.get_presentation_time_offset()
            presentation_time_offset = presentation_time_offset_sec * 10000000
            Utils.logger_.debug(self._session_id, "DashHandler::generate_subtitle_fragment presentation_time_offset={}".format(presentation_time_offset))

            actual_start_time = int((start_time - presentation_time_offset) / 10000000)
            actual_end_time = int((end_time - presentation_time_offset) / 10000000)

            Utils.logger_.info(self._session_id, "DashHandler::generate_subtitle_fragment actual_start_time={}, actual_end_time={}".format(actual_start_time, actual_end_time))

            for sub in subs:
                if (actual_start_time >= sub['start'] and actual_start_time < sub['end']) or \
                   (actual_end_time > sub['start'] and actual_end_time <= sub['end']) or \
                   (actual_start_time <= sub['start'] and actual_end_time >= sub['end']):

                    #print(Utils.seconds_to_webvtt_time(presentation_time_offset_sec + sub['start']))
                    #print(Utils.seconds_to_webvtt_time(presentation_time_offset_sec + sub['end']))
                    #print(sub['text'])
                    #webvtt_fragment += ('\n' + Utils.seconds_to_webvtt_time(sub['start']) + ' --> ' + Utils.seconds_to_webvtt_time(sub['end']) + '\n')
                    #webvtt_fragment += sub['text'] + '\n'

                    start_time_str = Utils.seconds_to_webvtt_time(presentation_time_offset_sec + sub['start'])
                    end_time_str = Utils.seconds_to_webvtt_time(presentation_time_offset_sec + sub['end'])

                    ttml_fragment += "      <p  region=\"r0\" style=\"s0\" begin=\"" + start_time_str + "\" end=\"" + end_time_str + "\" >"  #<span>"

                    ttml_string = sub['text']
                    ttml_string = ttml_string.replace('<', '')
                    ttml_string = ttml_string.replace('>', '')
                    ttml_string = ttml_string.replace('\n', '<br/>')

                    ttml_fragment += ttml_string

                    #</span>
                    ttml_fragment += "</p>\r\n"

        # closing tags
        ttml_fragment += "    </div>\r\n"  # /div
        ttml_fragment += "  </body>\r\n"  # /body
        ttml_fragment += "</tt>"  # /tt

        return ttml_fragment

    #################################
    # get_reference_manifest_url
    #################################
    def get_reference_manifest_url(self, dst_lang: EosLanguage) -> str:
        return ''

    #################################
    # get_fragments_list
    #################################
    def get_fragments_list(self, dst_lang: EosLanguage) -> List[EosFragment]:
        return self._reference_manifests[dst_lang.code_bcp_47()].fragments

    #################################
    # translate_subtitle_fragment
    #################################
    def translate_subtitle_fragment(self, src_fragment: bytes, src_next_fragment: bytes, src_language: EosLanguage, dst_language: EosLanguage):

        # print("type(src_fragment): ", type(src_fragment))
        # print("src_fragment: ", src_fragment.decode('utf-8'))

        dash_parser = DashFragmentParser(src_fragment)
        ttml = dash_parser.read_ttml()

        next_ttml = None
        if src_next_fragment is not None:
            next_dash_parser = DashFragmentParser(src_next_fragment)
            next_ttml = next_dash_parser.read_ttml()

        # print("ttml: ", ttml.decode('utf-8'))

        try:
            caption_set = DFXPReader().read(ttml.decode('utf-8'))
        except CaptionReadNoCaptions:
            Utils.logger_.error(self._session_id, "DashHandler::translate_subtitle_fragment error CaptionReadNoCaptions")
            return src_fragment
        
        next_caption_set = None
        if next_ttml is not None:
            try:
                next_caption_set = DFXPReader().read(next_ttml.decode('utf-8'))
            except CaptionReadNoCaptions:
                Utils.logger_.error(self._session_id, "DashHandler::translate_subtitle_fragment error CaptionReadNoCaptions")
                next_caption_set = None

        Utils.logger_.debug_color(self._session_id, "DashHandler::translate_subtitle_fragment original caption_set={}".format(caption_set._captions))
        caption_set = self._translate_caption_set(caption_set, next_caption_set, src_language, dst_language)
        Utils.logger_.debug_color(self._session_id, "DashHandler::translate_subtitle_fragment translated caption_set={}".format(caption_set._captions))
        
        modified_ttml = DFXPWriter().write(caption_set)
        # print("modified_ttml: ", modified_ttml)

        modified_fragment = dash_parser.update_ttml(modified_ttml.encode('utf-8'))

        return modified_fragment
