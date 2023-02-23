import os
#import ffmpeg
import subprocess
import shlex
from typing import List

import Utils as Utils
from CommonTypes import ExecutionTimer

global transcoder_
transcoder_ = None

# config variables
APP__FFMPEG_PATH = Utils.ConfigVariable('APP', 'FFMPEG_PATH', type=str, default_value='/usr/bin/ffmpeg', description='Path to ffmpeg', mandatory=False)
APP__SOX_PATH = Utils.ConfigVariable('APP', 'SOX_PATH', type=str, default_value='/usr/bin/sox', description='Path to sox', mandatory=False)


####################################################
#
#  Transcoder_
#
####################################################
class Transcoder_:
    __ffmpeg_location: str
    __sox_location: str

    ####################################################
    #  __init__
    ####################################################
    def __init__(self) -> None:

        self.__ffmpeg_location = APP__FFMPEG_PATH.value()
        self.__sox_location = APP__SOX_PATH.value()

    ####################################################
    #  add_WAV_header
    ####################################################
    def add_WAV_header(self,
                       file_in: str,
                       file_out: str,
                       sample_rate: int,
                       bits_per_sample: int,
                       channels: int) -> None:

        data_size = os.path.getsize(file_in)

        wav_header = bytes("RIFF", 'ascii')  # (4byte) Marks file as RIFF
        wav_header += (data_size + 36).to_bytes(4, 'little')  # (4byte) File size in bytes excluding this and RIFF marker
        wav_header += bytes("WAVE", 'ascii')  # (4byte) File type
        wav_header += bytes("fmt ", 'ascii')  # (4byte) Format Chunk Marker
        wav_header += (16).to_bytes(4, 'little')  # (4byte) Length of above format data
        wav_header += (1).to_bytes(2, 'little')  # (2byte) Format type (1 - PCM)
        wav_header += (channels).to_bytes(2, 'little')  # (2byte)
        wav_header += (sample_rate).to_bytes(4, 'little')  # (4byte)
        wav_header += (sample_rate * channels * bits_per_sample // 8).to_bytes(4, 'little')  # (4byte)
        wav_header += (channels * bits_per_sample // 8).to_bytes(2, 'little')  # (2byte)
        wav_header += (bits_per_sample).to_bytes(2, 'little')  # (2byte)
        wav_header += bytes("data", 'ascii')  # (4byte) Data Chunk Marker
        wav_header += (data_size).to_bytes(4, 'little')  # (4byte) Data size in bytes

        with open(file_in, "rb") as f_in, open(file_out, "wb") as f_out:
            f_out.write(wav_header)
            for chunk in iter(lambda: f_in.read(1024), b""):
                f_out.write(chunk)

    ####################################################
    #  transcode_file
    ####################################################
    def transcode_file(self,
                       file_in: str,
                       file_out: str,
                       sampling_freq_out_hz: int) -> None:

        codec_params_in = ''
        codec_params_out = '-f s16le -acodec pcm_s16le'
        audio_filters = ''

        Utils.logger_.dump('Transcoder_', "transcode_file file_in: {}, file_out: {}".format(file_in, file_out))

        #print("transcode_file file={}".format(file_in))

        #try:
        #    with ExecutionTimer('Transcoder_.transcode_file'):
        #        out, err = (ffmpeg
        #            .input(file_in)
        #            .output(file_out, ar=str(sampling_freq_out_hz), ac=1, f='s16le', acodec='pcm_s16le')
        #            .overwrite_output()
        #            .global_args('-hide_banner', '-loglevel', 'quiet')
        #            .run(capture_stdout=True, capture_stderr=True))

                    # for flac, output kwargs: acodec='flac', sample_fmt='s16'

        #except ffmpeg.Error as e:
        #    print(e.stderr)

        #print("transcode_file file={}, len={} \n\n\n".format(file_out, len(out)))
 
        with ExecutionTimer('Transcoder_.transcode_file'):

            # in_codec_params =  '-f s16le'
            # out_codec_params = '-f wav -acodec pcm_s16le'

            # ffmpeg_audio_filters = ''
            # ffmpeg_audio_filters = ' -af \"highpass=f=200, lowpass=f=3000, dynaudnorm, treble=g=12:f=4000:width_type=h:width=1000\" '
            # ffmpeg_audio_filters = ' -af \"treble=g=9\" '

            ffmpeg_command = self.__ffmpeg_location + ' -y -hide_banner -loglevel quiet ' + codec_params_in + ' -i ' + file_in + ' ' + codec_params_out + \
                ' -ar ' + str(sampling_freq_out_hz) + ' -ac 1 ' + audio_filters + ' ' + file_out

            Utils.logger_.dump('Transcoder_', "Transcoder_::transcode_file ffmpeg_command: {}".format(ffmpeg_command))

            # subprocess.call(ffmpeg_command, shell=True)
            subprocess.call(shlex.split(ffmpeg_command))

    #################################
    #  get_first_pts
    #################################
    def get_first_pts(self, file_in: str) -> int:

        #first_pts = 0

        #print("get_first_pts file={}".format(file_in))

        #try:
        #    probe = ffmpeg.probe(file_in, select_streams='v', show_frames=None, read_intervals='%+0.01')
        #    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            # print("video_stream: ", video_stream)
        #    first_pts = int(video_stream['start_pts'])

        #except ffmpeg.Error as e:
        #    print("error:", e.stderr)

        #print("get_first_pts file={}, first_pts={} \n\n\n".format(file_in, first_pts))

        #return first_pts

        #ffprobe_command = "/usr/bin/ffprobe -v warning -select_streams v -show_frames -read_intervals %+0.01 " + file_in
        ffprobe_command = "/usr/bin/ffprobe -loglevel quiet -show_frames -read_intervals %+0.01 " + file_in

        Utils.logger_.dump('Transcoder_', "Transcoder_::get_first_pts ffmpeg_command: {}".format(ffprobe_command))

        _command = shlex.split(ffprobe_command)
        Utils.logger_.debug('EosFragment', "EosFragment::get_first_pts _command".format(_command))

        output = subprocess.check_output(_command, encoding='utf8').split("\n")
        for line in output:
            if ('pkt_pts=' in line):
                first_pts = line[8:]
                return first_pts

        return 0

    ####################################################
    #  extract_audio
    ####################################################
    def extract_audio(self,
                      file_in: str,
                      file_out: str) -> None:

        Utils.logger_.dump('Transcoder_', "Transcoder_::extract_audio extract_audio file_in: {}, file_out: {}".format(file_in, file_out))

        ffmpeg_command = self.__ffmpeg_location + ' -y -hide_banner -loglevel quiet ' + ' -i ' + file_in + ' ' + ' -vn -acodec copy ' + file_out

        Utils.logger_.dump('Transcoder_', "Transcoder_::extract_audio ffmpeg_command: {}".format(ffmpeg_command))

        subprocess.call(shlex.split(ffmpeg_command))


	#print("extract_audio file={}".format(file_out))

        #try:
        #    out, err = (ffmpeg
        #        .input(file_in)
        #        .output(file_out)
        #        .overwrite_output()
        #        .global_args('-hide_banner', '-loglevel', 'quiet')
        #        .run(capture_stdout=True, capture_stderr=True))
        #except ffmpeg.Error as e:
        #    print(e.stderr)

        #print("extract_audio file={}, len={} \n\n\n".format(file_out, len(out)))

        # return out

    ####################################################
    #  concatenate_files
    ####################################################
    def concatenate_files(self,
                          files_in: List[str],
                          file_out: str) -> None:

        with ExecutionTimer('Transcoder_.concatenate_files'):

            sox_command = self.__sox_location + ' ' +  ' '.join(files_in) + ' ' + file_out

            print("sox_command: ", sox_command)

            # subprocess.call(ffmpeg_command, shell=True)
            subprocess.call(shlex.split(sox_command))


####################################################
#  init_transcoder
####################################################
def init_transcoder() -> None:

    global transcoder_
    transcoder_ = Transcoder_()
