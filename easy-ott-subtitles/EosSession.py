import base64
import requests
import uuid
import datetime
from typing import Optional, List, Dict, Any

import Utils as Utils
from OttHandler import OttProtocols, OttHandler, HlsHandler, DashHandler
from EosRequestResponse import EosSessionRequest, EosManagementRequest, EosSessionResponse
from EosTranscribeStream import EosTranscribeStream, EosTranscribeLiveStream
from CommonTypes import EosHttpConfig, EosNames
from Languages import EosLanguages, EosLanguage

STREAMING_SERVER__USE_HTTPS = Utils.ConfigVariable('STREAMING_SERVER', 'USE_HTTPS', type=bool, default_value=False, description='Use HTTPS fo streaming', mandatory=False)
STREAMING_SERVER__HOST_NAME = Utils.ConfigVariable('STREAMING_SERVER', 'HOST_NAME', type=str, default_value='127.0.0.1', description='Host name', mandatory=False)


####################################################
#
#  EosSession
#
####################################################
class EosSession:
    _session_id: str
    _ott_protocol: OttProtocols
    _live: bool
    _dst_languages: List[EosLanguage]
    _src_language: EosLanguage
    _variants: List[int]
    _ott_handler: OttHandler
    _session_url_base64: str  # the original url in base64 url-safe encoding
    _variant_manifest_url: str  # url of the original manifest
    _manifest_requested: bool
    _subtitles_manifest_requested: bool
    _default_lang_code: Optional[str]
    _audio_manifest: str
    _variant_manifest: Optional[str]  # cached variant manifest
    _variant_manifest_content_type: Optional[str]  # cached variant manifest content type
    _subtitles_manifest: Dict[str, str]  # map dst_lang->cahched subtitle manifest
    _subtitles_manifest_content_type: Optional[str]  # cached subtitle manifest content type
    _requests_counter: int

    #################################
    # __init__
    #################################
    def __init__(self, session_url: str, ott_protocol: OttProtocols, live: bool,
                 dst_languages: List[str], src_language: str, variants: List[int]) -> None:

        # create session id
        # TODO: maybe change to base64(origin_url+dst_langs), to allow migration of the session to another node
        self._session_id = str(uuid.uuid4())

        # create ott handler accroding to OTT protocol
        self._ott_protocol = ott_protocol
        if self._ott_protocol is OttProtocols.HLS_PROTOCOL:
            self._ott_handler = HlsHandler(self._session_id, live)
        elif self._ott_protocol is OttProtocols.DASH_PROTOCOL:
            self._ott_handler = DashHandler(self._session_id, live)
        else:
            Utils.logger_.error(str(self._session_id), "EosSession::__init__ invalid ott_protocol={}".format(ott_protocol))
            return

        # live / vod
        self._live = live

        # languges
        self._src_language = EosLanguages().find(src_language)
        self._dst_languages = []
        for dst_lang in dst_languages:
            self._dst_languages.append(EosLanguages().find(dst_lang))

        self._variants = variants

        # origin url
        self._session_url_base64 = session_url
        Utils.logger_.info(str(self._session_id), "EosSession::__init__ self._session_url_base64={}".format(self._session_url_base64))

        self._variant_manifest_url = base64.urlsafe_b64decode(self._session_url_base64).decode('utf-8')
        Utils.logger_.info(str(self._session_id), "EosSession::__init__ self._variant_manifest_url={}".format(self._variant_manifest_url))

        # initialize member variables
        self._default_lang_code = None
        self._manifest_requested = False
        self._subtitles_manifest_requested = False
        self._audio_manifest = ''

        self._variant_manifest = {}
        self._variant_manifest_content_type = None
        self._subtitles_manifest = {}
        self._subtitles_manifest_content_type = None

        self._requests_counter = 0

    #################################
    # set_default_lang
    #################################
    def set_default_lang(self, default_language) -> None:

        default_lang = EosLanguages().find(default_language)
        if default_lang is not None:
            self._default_lang_code = default_lang.code_bcp_47()

    #################################
    # get_streaming_url
    #################################
    def get_streaming_url(self) -> str:

        use_https = STREAMING_SERVER__USE_HTTPS.value()
        host_name = STREAMING_SERVER__HOST_NAME.value()

        streaming_url = ''
        if use_https is True:
            streaming_url += 'https://'
        else:
            streaming_url += 'http://'

        streaming_url += host_name + '/'
        streaming_url += EosNames.service_name + '/'
        streaming_url += 'v1' + '/'
        streaming_url += self.get_session_id() + '/'
        streaming_url += EosNames.variant_manifest_postfix + '.'
        streaming_url += OttProtocols.extension_string(self._ott_protocol)

        return streaming_url

    #################################
    # get_session_id
    #################################
    def get_session_id(self) -> str:
        return self._session_id

    #################################
    # get_protocol
    #################################
    def get_protocol(self) -> OttProtocols:
        return self._ott_protocol

    #################################
    # is_live
    #################################
    def is_live(self) -> bool:
        return self._live

    #################################
    # get_session_url_base64
    #################################
    def get_session_url_base64(self) -> str:
        return self._session_url_base64

    #################################
    #  on_request
    #################################
    def on_request(self, request: EosSessionRequest) -> EosSessionResponse:

        # which type of request is this? manifest or fragment?
        if request.is_eos_manifest_request() is True:
            return self.on_subtitle_request(request)

        if request.is_live_manifest_request() is True:
            return self.on_live_manifest_request(request)

        if request.is_variant_manifest_request() is True:
            return self.on_manifest_request(request)

        response = EosSessionResponse()
        response.error = "unknown requests"
        return response

    #################################
    #  on_subtitle_request
    #################################
    def on_subtitle_request(self, request: EosSessionRequest) -> EosSessionResponse:

        # if variant manifest was not requested, create a request for the variant manifest and continue
        if self._manifest_requested is False:
            self.on_manifest_request(request)

        if self._ott_protocol == OttProtocols.HLS_PROTOCOL and request.is_fragment_request() is False:
            return self.on_subtitle_manifest_request(request)
        else:
            if self._ott_protocol == OttProtocols.HLS_PROTOCOL and self._subtitles_manifest_requested is False:
                self.on_subtitle_manifest_request(request)

            return self.on_subtitle_fragment_request(request)

    #################################
    #  on_subtitle_fragment_request
    #################################
    def on_subtitle_fragment_request(self, request: EosSessionRequest) -> EosSessionResponse:
        Utils.logger_.debug(str(self._session_id), "EosSession::on_subtitle_fragment_request request.path={}".format(request.path()))

        response = self.prepare_subtitle_fragment(request, self._src_language)

        return response

    #################################
    #  on_subtitle_manifest_request
    #  only for HLS
    #################################
    def on_subtitle_manifest_request(self, request: EosSessionRequest) -> EosSessionResponse:

        Utils.logger_.debug(str(self._session_id), "EosSession::on_subtitle_manifest_request request.path={}, request.dst_lang={}".format(request.path(), request.dst_lang()))

        # cache only non-live sessions
        # TODO: cache live manifest for few seconds
        if self._live is False:
            if request.dst_lang() in self._subtitles_manifest:
                response = EosSessionResponse()
                response.response = self._subtitles_manifest[request.dst_lang()]
                response.content_type = self._subtitles_manifest_content_type
                return response

        response = self.prepare_subtitle_manifest(request)

        if response.response is not None:
            self._subtitles_manifest[request.dst_lang()] = response.response
            self._subtitles_manifest_content_type = response.content_type

            self._subtitles_manifest_requested = True

        return response

    #################################
    #  on_live_manifest_request
    #################################
    def on_live_manifest_request(self, request: EosSessionRequest) -> EosSessionResponse:

        live_manifest, fragment_list = self._ott_handler.get_live_manifest(request.live_origin_manifest_url())

        content_type = self._ott_handler.get_manifest_content_type()

        response = EosSessionResponse()
        response.response = str.encode(live_manifest)
        response.content_type = content_type
        response.cache = False

        return response

    #################################
    #  on_manifest_request
    #################################
    def on_manifest_request(self, request: EosSessionRequest) -> EosSessionResponse:

        Utils.logger_.debug(str(self._session_id), "EosSession::on_manifest_request self._variant_manifest_url={}".format(self._variant_manifest_url))

        default_lang: Optional[EosLanguage] = None
        default_lang_code = 'none'
        if request.default_language() != '':
            default_lang = EosLanguages().find(request.default_language())
            if default_lang is not None:
                default_lang_code = default_lang.code_bcp_47()
        if default_lang_code == 'none':
            if self._default_lang_code is not None:
                default_lang_code = self._default_lang_code
                default_lang = EosLanguages().find(self._default_lang_code)

        # is this the first manifest request (was it generated already?)
        if self._manifest_requested is True:
            # check the default language, if it matches
            if default_lang_code in self._variant_manifest.keys():
                # if this is HLS (variant manifest is static) or non-live (hls/dash manifests are static)
                if self._ott_protocol is OttProtocols.HLS_PROTOCOL or self._live is False:
                    response = EosSessionResponse()
                    response.response = self._variant_manifest[default_lang_code]
                    response.content_type = self._variant_manifest_content_type
                    return response
                # if it is dash live, we need to update the manifest with the updated framgents
                elif self._ott_protocol is OttProtocols.DASH_PROTOCOL and self._live is True:
                    response = EosSessionResponse()
                    response.response = str.encode(self._ott_handler.build_manifest())
                    response.content_type = self._variant_manifest_content_type
                    response.cache = False  # TODO: change to 1 fragment interval
                    return response
            # different default language
            else:
                self._ott_handler.set_default_langauge(default_lang)
                response = EosSessionResponse()
                response.response = str.encode(self._ott_handler.build_manifest())
                response.content_type = self._ott_handler.get_manifest_content_type()
                self._variant_manifest[default_lang_code] = response.response
                return response

        try:
            headers = {'User-Agent': EosHttpConfig.user_agent}
            responses = requests.get(self._variant_manifest_url, headers=headers)

            if responses.status_code == requests.codes.ok:

                original_manifest = responses.text
                # Utils.logger_.dump(str(self._session_id), 'EosSession::on_manifest_request original_manifest={}'.format(original_manifest))

            else:
                Utils.logger_.error(str(self._session_id), "EosSession::on_manifest_request error getting manifest from server ({})".format(responses.status_code))
                print(responses)
                response = EosSessionResponse()
                response.error = str(responses.status_code)
                return response

        except requests.ConnectionError:
            error_str = "EosSession::on_manifest_request Error connecting to server {}".format(self._variant_manifest_url)
            Utils.logger_.error(str(self._session_id), error_str)
            response = EosSessionResponse()
            response.error = "Error connecting to server {}".format(self._variant_manifest_url)
            return response

        if(responses.url != self._variant_manifest_url):
            self._variant_manifest_url = responses.url
            Utils.logger_.debug_color(str(self._session_id), "EosSession::on_manifest_request ---redirected--- self._variant_manifest_url={}".format(self._variant_manifest_url))

        self._ott_handler.parse_manifest(original_manifest, self._variant_manifest_url, self._variants)

        # print("--------------------------------------------------------- self._live: ", self._live)

        if self._live is True:
            self._ott_handler.redirect_urls(self._src_language.live_delay_seconds())
        else:
            self._ott_handler.make_urls_absolute()

        self.add_subtitle_stream(default_lang)

        self._manifest_requested = True

        response = EosSessionResponse()
        response.response = str.encode(self._ott_handler.build_manifest())
        response.content_type = self._ott_handler.get_manifest_content_type()

        if self._ott_protocol is OttProtocols.DASH_PROTOCOL and self._live is True:
            response.cache = False

        # cache if asked again
        self._variant_manifest[default_lang_code] = response.response
        self._variant_manifest_content_type = response.content_type

        if self._live is True:
            self.start_live()
        else:
            self.start_vod()

        return response

    #################################
    #  start_live
    #################################
    def start_live(self) -> bool:
        return True

    #################################
    #  start_vod
    #################################
    def start_vod(self) -> None:
        pass

    #################################
    #  count_request
    #################################
    def count_request(self, tokens: List[str]) -> None:

        Utils.logger_.debug(str(self._session_id), "EosSession::count_request tokens={}".format(tokens))

        self._requests_counter += 1

    #################################
    #  get_status
    #################################
    def get_status(self) -> Dict[str, Any]:

        Utils.logger_.debug(str(self._session_id), "EosSession::get_status")

        state_str = self._get_state()

        time_seconds = self._get_engine_time()
        time_seconds_int = int(time_seconds)
        time_str = str(datetime.timedelta(seconds=time_seconds_int))

        accuracy_str = "{:.2f}%".format(self._get_engine_accuracy())

        status_reply = {'state': state_str,
                        'requests': self._requests_counter,
                        #'warnings': ['warning1', 'warning2'],
                        'time': time_str,
                        'accuracy': accuracy_str}

        return status_reply

    #################################
    #  disable
    #################################
    def disable(self) -> Dict[str, Any]:

        Utils.logger_.debug(str(self._session_id), "EosSession::disable")

        self.pause_transcribe()

        status_reply = {'state': 'disabled'}

        return status_reply

    #################################
    #  enable
    #################################
    def enable(self) -> Dict[str, Any]:

        Utils.logger_.debug(str(self._session_id), "EosSession::enable")

        self.resume_transcribe()

        status_reply = {'state': 'enabled'}

        return status_reply

    #################################
    #  close
    #################################
    def close(self) -> Dict[str, Any]:

        Utils.logger_.debug(str(self._session_id), "EosSession::close")

        self._ott_handler.close()

        self.close_transcribe()

        status_reply = {'state': 'close'}

        return status_reply


####################################################
#
#  EosTranslateSession
#
####################################################
class EosTranslateSession(EosSession):

    #################################
    # __init__
    #################################
    def __init__(self, session_url: str, ott_protocol: OttProtocols, live: bool,
                 dst_languages: List[str], src_language: str, variants: List[int]) -> None:

        EosSession.__init__(self, session_url, ott_protocol, live, dst_languages, src_language, variants)

    #################################
    # _get_session_type
    #################################
    def _get_session_type(self) -> str:
        return 'translate'

    #################################
    # add_subtitle_stream
    #################################
    def add_subtitle_stream(self, default_lang: EosLanguage) -> None:

        for dst_lang in self._dst_languages:
            self._ott_handler.clone_subtitle_stream(self._src_language, dst_lang, default_lang)

    #################################
    # prepare_subtitle_manifest
    # only for HLS
    #################################
    def prepare_subtitle_manifest(self, request: EosSessionRequest) -> EosSessionResponse:

        reference_manifest_url = base64.urlsafe_b64decode(request.reference_manifest_url()).decode('utf-8')

        Utils.logger_.dump(str(self._session_id), "EosTranslateSession::prepare_subtitle_manifest reference_manifest_url={}".format(reference_manifest_url))

        try:
            headers = {'User-Agent': EosHttpConfig.user_agent}
            responses = requests.get(reference_manifest_url, headers=headers)

            if responses.status_code == requests.codes.ok:

                original_manifest = responses.text
                Utils.logger_.dump(str(self._session_id), 'EosTranslateSession::prepare_subtitle_manifest original_manifest={}'.format(original_manifest))

            else:
                Utils.logger_.error(str(self._session_id), "EosTranslateSession::prepare_subtitle_manifest error getting manifest from server ({})".format(responses.status_code))
                response = EosSessionResponse()
                response.error = 'error'
                return response

        except requests.ConnectionError:
            error_str = "EosTranslateSession::prepare_subtitle_manifest Error connecting to server {}".format(reference_manifest_url)
            Utils.logger_.error(str(self._session_id), error_str)
            response = EosSessionResponse()
            response.error = error_str
            return response

        response = EosSessionResponse()
        response.response = str.encode(self._ott_handler.clone_reference_manifest(original_manifest, request.dst_lang(), reference_manifest_url))
        response.content_type = responses.headers['Content-Type']

        return response

    #################################
    # prepare_subtitle_fragment
    #################################
    def prepare_subtitle_fragment(self, request: EosSessionRequest, src_language: EosLanguage) -> EosSessionResponse:

        if self._ott_protocol == OttProtocols.HLS_PROTOCOL:
            # print("request.reference_fragment_url()", request.reference_fragment_url())
            reference_fragment_url = base64.urlsafe_b64decode(request.reference_fragment_url()).decode('utf-8')
        else:
            # print("request.dash_fragment_url()", request.dash_fragment_url())
            # print("self._variant_manifest_url", self._variant_manifest_url)
            reference_fragment_url = self._variant_manifest_url[:self._variant_manifest_url.rfind('/') + 1] + request.dash_fragment_url()

        Utils.logger_.dump(str(self._session_id), "EosTranslateSession::prepare_subtitle_fragment reference_fragment_url={}".format(reference_fragment_url))

        try:
            headers = {'User-Agent': EosHttpConfig.user_agent}
            responses = requests.get(reference_fragment_url, headers=headers)

            if responses.status_code == requests.codes.ok:

                original_fragment = responses.content
                Utils.logger_.dump(str(self._session_id), 'EosTranslateSession::prepare_subtitle_fragment original_fragment={}'.format(original_fragment))

            else:
                error_str = "EosTranslateSession::prepare_subtitle_fragment error getting fragment from server ({})".format(responses.status_code)
                Utils.logger_.error(str(self._session_id), error_str)
                response = EosSessionResponse()
                response.error = error_str
                return response

        except requests.ConnectionError:
            error_str = "EosTranslateSession::prepare_subtitle_fragment Error connecting to server {}".format(reference_fragment_url)
            Utils.logger_.error(str(self._session_id), error_str)
            response = EosSessionResponse()
            response.error = error_str
            return response

        dst_language = EosLanguages().find(request.dst_lang())

        response = EosSessionResponse()
        if self._ott_protocol == OttProtocols.DASH_PROTOCOL and request.dash_fragment_url().find("=Init") != -1:
            response.response = original_fragment
        else:
            response.response = self._ott_handler.translate_subtitle_fragment(original_fragment, self._src_language, dst_language)
        response.content_type = responses.headers['Content-Type']

        return response

    #################################
    # _get_state
    #################################
    def _get_state(self) -> str:
        return 'active'

    #################################
    # _get_engine_time
    #################################
    def _get_engine_time(self) -> float:
        return 0

    #################################
    # _get_engine_accuracy
    #################################
    def _get_engine_accuracy(self) -> float:
        return 0

    #################################
    # pause_transcribe
    #################################
    def pause_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranslateSession::pause_transcribe")

    #################################
    # resume_transcribe
    #################################
    def resume_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranslateSession::resume_transcribe")

    #################################
    # close_transcribe
    #################################
    def close_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranslateSession::close_transcribe")


####################################################
#
#  EosTranscribeSession
#
####################################################
class EosTranscribeSession(EosSession):
    # _fragments: Dict[str, EosFragment]  # [original_url, fragment]

    #################################
    # __init__
    #################################
    def __init__(self, session_url: str, ott_protocol: OttProtocols, live: bool,
                 dst_languages: List[str], src_language: str, variants: List[int]) -> None:

        EosSession.__init__(self, session_url, ott_protocol, live, dst_languages, src_language, variants)

        if src_language not in dst_languages:
            Utils.logger_.info(str(self._session_id), "EosTranscribeSession::__init__ source language must be one of the destination languages".format(self._live))
            self._dst_languages.append(EosLanguages().find(src_language))
            
        self._transcribe_session = None

    #################################
    # _get_session_type
    #################################
    def _get_session_type(self) -> str:
        return 'transcribe'

    #################################
    # add_subtitle_stream
    #################################
    def add_subtitle_stream(self, default_lang: EosLanguage) -> None:

        for dst_lang in self._dst_languages:
            self._ott_handler.add_subtitle_stream(self._src_language, dst_lang, default_lang)

    #################################
    # prepare_subtitle_manifest
    # only for HLS
    #################################
    def prepare_subtitle_manifest(self, request: EosSessionRequest) -> EosSessionResponse:

        if self._live is False:

            reference_manifest_url = base64.urlsafe_b64decode(request.reference_manifest_url()).decode('utf-8')

            Utils.logger_.dump(str(self._session_id), "EosTranscribeSession::prepare_subtitle_manifest reference_manifest_url={}".format(reference_manifest_url))

            try:
                headers = {'User-Agent': EosHttpConfig.user_agent}
                responses = requests.get(reference_manifest_url, headers=headers)

                if responses.status_code == requests.codes.ok:

                    original_manifest = responses.text
                    Utils.logger_.dump(str(self._session_id), 'EosTranscribeSession::prepare_subtitle_manifest original_manifest={}'.format(original_manifest))

                else:
                    Utils.logger_.error(str(self._session_id), "EosTranscribeSession::prepare_subtitle_manifest error getting manifest from server ({})".format(responses.status_code))
                    response = EosSessionResponse()
                    response.error = 'error'
                    return response

            except requests.ConnectionError:
                error_str = "EosTranscribeSession::prepare_subtitle_manifest Error connecting to server {}".format(reference_manifest_url)
                Utils.logger_.error(str(self._session_id), error_str)
                response = EosSessionResponse()
                response.error = error_str
                return response

            response = EosSessionResponse()
            response.response = str.encode(self._ott_handler.clone_reference_manifest(original_manifest, request.dst_lang(), reference_manifest_url))
            response.content_type = responses.headers['Content-Type']

            return response

        else:  # live

            live_manifest, fragment_list = self._ott_handler.get_live_manifest(request.reference_manifest_url())

            response = EosSessionResponse()
            response.response = str.encode(self._ott_handler.build_live_subtitle_manifest(fragment_list))
            response.content_type = self._ott_handler.get_manifest_content_type()
            response.cache = False

            return response

    #################################
    #  start_live
    #################################
    def start_live(self) -> bool:

        Utils.logger_.debug(str(self._session_id), "EosSession::start_live")

        # start live manifest handling for all sub streams
        request = EosManagementRequest(path=self._session_url_base64,
                                        ott_protocol=self._ott_protocol,
                                        live=self._live)
        response: EosSessionResponse = self.on_manifest_request(request)
        if response.is_error() is True:
            Utils.logger_.error(str(self._session_id), "EosSession::start_live error getting manifest")
            return False

        # start transcribing
        # only implemented in transcribe session
        self.start_transcribe(self._src_language, self._dst_languages)

        return True

    #################################
    #  start_vod
    #################################
    def start_vod(self) -> None:

        Utils.logger_.debug(str(self._session_id), "EosTranscribeSession::start_vod")

        # make sure fragment lists are generated
        request = EosManagementRequest(path=self._session_url_base64,
                                        ott_protocol=self._ott_protocol,
                                        live=self._live)
        self.on_manifest_request(request)

        request = EosManagementRequest(path=self._session_url_base64,
                                        ott_protocol=self._ott_protocol,
                                        live=self._live,
                                        src_lang=self._src_language.code_bcp_47(),
                                        dst_lang=self._dst_languages[0].code_bcp_47(),
                                        fragment_request=True,
                                        reference_manifest_url=self._ott_handler.get_reference_manifest_url(self._dst_languages[0]))
        self.on_subtitle_request(request)

        # start transcribing
        # only implemented in transcribe session
        self.start_transcribe(self._src_language, self._dst_languages)

    #################################
    #  count_request
    #################################
    def count_request(self, tokens: List[str]) -> None:

        Utils.logger_.debug(str(self._session_id), "EosTranscribeSession::count_request tokens={}".format(tokens))

        self._requests_counter += 1
    
    #################################
    # start_transcribe
    #################################
    def start_transcribe(self, src_lang: EosLanguage, dst_langs: List[EosLanguage]) -> None:

        if self._transcribe_session is not None:
            return

        Utils.logger_.info(str(self._session_id), "EosTranscribeSession::start_transcribe self._live={}".format(self._live))

        if self._live is False:
            self._transcribe_session = EosTranscribeStream(self._session_id,
                                                            self._ott_protocol,
                                                            src_lang,
                                                            dst_langs,
                                                            self._ott_handler.get_fragments_list(dst_langs[0]),
                                                            16000)  # sample_rate

            self._transcribe_session.start()
        else:
            self._transcribe_session = EosTranscribeLiveStream(self._session_id,
                                                                self._ott_protocol,
                                                                src_lang,
                                                                dst_langs,
                                                                16000)  # sample_rate

            self._transcribe_session.start()

            self._ott_handler.register_live_parser_listener(dst_langs[0].code_bcp_47(), self._transcribe_session)

    #################################
    # pause_transcribe
    #################################
    def pause_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranscribeSession::pause_transcribe self._live={}".format(self._live))

        if self._transcribe_session is None:
            return

        self._transcribe_session.pause()

    #################################
    # resume_transcribe
    #################################
    def resume_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranscribeSession::resume_transcribe self._live={}".format(self._live))

        if self._transcribe_session is None:
            return

        self._transcribe_session.resume()

    #################################
    # close_transcribe
    #################################
    def close_transcribe(self):

        Utils.logger_.info(str(self._session_id), "EosTranscribeSession::close_transcribe self._live={}".format(self._live))

        if self._transcribe_session is None:
            return

        self._transcribe_session.close()
        self._transcribe_session = None

    #################################
    # prepare_subtitle_fragment
    #################################
    def prepare_subtitle_fragment(self, request: EosSessionRequest, src_language: EosLanguage) -> EosSessionResponse:

        if self._ott_protocol is OttProtocols.HLS_PROTOCOL:
            start_time, end_time = self._ott_handler.get_start_stop_times(request.dst_lang(), request.reference_fragment_url())

            Utils.logger_.dump(str(self._session_id), "EosTranscribeSession::prepare_subtitle_fragment request.reference_fragment_url()={}, start_time={}, end_time={}".format(request.reference_fragment_url(), start_time, end_time))

            subs: List[Dict[str, Any]] = self._transcribe_session.get_subs(request.dst_lang())

            #print(subs)
            #print("")
            #for sub in subs:
            #    print(sub)
            #if len(subs) > 0:
            #    print("last sub: ", subs[-1])

            first_pts = None
            first_start_time = None
            if self._live is True:
                first_pts, first_start_time = self._transcribe_session.get_start_times()

            subtitle_fragment = self._ott_handler.generate_subtitle_fragment(start_time, end_time, subs, first_pts, first_start_time)

            #print(subtitle_fragment)

            response = EosSessionResponse()
            response.response = subtitle_fragment.encode('utf-8')
            response.content_type = 'binary/octet-stream'

            return response

        else:
            timestamp = request.dash_timestamp()
            if timestamp == 'Init':
                subtitle_fragment = self._ott_handler.generate_init_fragment()
            else:
                start_time = int(timestamp)
                end_time = start_time + 4000

                subs: List[Dict[str, Any]] = self._transcribe_session.get_subs(request.dst_lang())

                #print(subs)
                #for sub in subs:
                #    print(sub)

                subtitle_ttml = self._ott_handler.generate_subtitle_fragment(start_time, end_time, subs)

                #print(subtitle_ttml)

                subtitle_fragment = self._ott_handler.pack_subtitle_fragment(start_time, end_time, subtitle_ttml)

            response = EosSessionResponse()
            response.response = subtitle_fragment
            response.content_type = 'binary/octet-stream'

            return response

    #################################
    # _get_state
    #################################
    def _get_state(self) -> str:
        if self._transcribe_session is None:
            return 'not active'

        return self._transcribe_session.get_state()

    #################################
    # _get_engine_time
    #################################
    def _get_engine_time(self) -> float:
        if self._transcribe_session is None:
            return 0

        return self._transcribe_session.get_engine_time()

    #################################
    # _get_engine_accuracy
    #################################
    def _get_engine_accuracy(self) -> float:
        if self._transcribe_session is None:
            return 0

        return self._transcribe_session.get_engine_accuracy()
