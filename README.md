# easy-ott-subtitles
OTT (DASH/HLS) automatic subtitles translator/transcriber.
Suitable to VoD and Live OTT streams.

It is basically a manifest manipulator for HLS and DASH. It adds subtitle(s) track(s) to the origin manifest according to the URL parameters (see instructions below).
It allows very easy intergation: no changes to the origin stream are needed, work is done on-the-fly.
Original Video/Audio/Text tracks are directed to the origin server. Only added text tracks are served from easy-ott-subtitles server.

Currently using Google Cloud Translation and Speech-to-Text APIs.

* * *

## Installation

* Create virtual enviroment
```bash
$ virtualenv -p python3 venv
$ source venv/bin/activate
```

* Install python dependencies
```bash
$ pip install -r requirements.txt
```

* Install FFmpeg (used for transcribe only)
```bash
$ sudo apt-get install ffmpeg
```

* For python 3.10, fix this in venv/lib/python3.10/site-packages/construct/core.py:
-import collections
+#import collections
+import collections.abc as collections

* * *

## Configuration
* Configure Google Cloud service account key
1. Generate service account key json file.
2. Copy the file to folder 'google_key'
3. Configure project id and service account key file path in ini file under GOOGLE_API:
```bash
[GOOGLE_API]
PROJECT_ID = <project_id>
SERVICE_ACCOUNT_FILE = google_key/<service_account_key_file_path>
```

* Default port number is 8500.
This can be changed in ini file under HTTP_SERVER:
```bash
[HTTP_SERVER]
EOS_HTTP_PORT_NUMBER = 8500
```
* * *

## Usage

* Start the server
```bash
$ python easy-ott-subtitles -c eos.ini
```

* * *

## URL Generation

* To consturct a playout URL, use this formula:
```bash
http://<server_ip_address>:<server_port_number>/eos/v1/<dash/hls>/<vod/live>/<translate/transcribe>/<source_language>/<origin_stream_url_base64>/eos_manifest.<m3u8/mpd>?languages=<destination_languages>&default=<default_language>
```

### Example

#### Transcribe

* Use a sample HLS test stream in English: 
http://amssamples.streaming.mediaservices.windows.net/91492735-c523-432b-ba01-faba6c2206a2/AzureMediaServicesPromo.ism/manifest(format=m3u8-aapl)

* This stream does not contain any subtitles which we can translate, so we nee to transcribe it.

* Encoded stream URL to Base64: aHR0cDovL2Ftc3NhbXBsZXMuc3RyZWFtaW5nLm1lZGlhc2VydmljZXMud2luZG93cy5uZXQvOTE0OTI3MzUtYzUyMy00MzJiLWJhMDEtZmFiYTZjMjIwNmEyL0F6dXJlTWVkaWFTZXJ2aWNlc1Byb21vLmlzbS9tYW5pZmVzdChmb3JtYXQ9bTN1OC1hYXBsKQ==

* Construct playout URL with English and German subtitles:
http://127.0.0.1:8500/eos/v1/hls/vod/transcribe/en-US/aHR0cDovL2Ftc3NhbXBsZXMuc3RyZWFtaW5nLm1lZGlhc2VydmljZXMud2luZG93cy5uZXQvOTE0OTI3MzUtYzUyMy00MzJiLWJhMDEtZmFiYTZjMjIwNmEyL0F6dXJlTWVkaWFTZXJ2aWNlc1Byb21vLmlzbS9tYW5pZmVzdChmb3JtYXQ9bTN1OC1hYXBsKQ==/eos_manifest.m3u8?languages=en-US,de-DE&default=en-US

* You need to wait a bit (~30 seconds) before start playing, to let the transcribe service to get ahead with the Speech-to-Text process.

#### Translate

* Use a sample HLS test stream with English subtitles:
http://sample.vodobox.com/planete_interdite/planete_interdite_alternate.m3u8

* Encoded stream URL to Base64: 
aHR0cDovL3NhbXBsZS52b2RvYm94LmNvbS9wbGFuZXRlX2ludGVyZGl0ZS9wbGFuZXRlX2ludGVyZGl0ZV9hbHRlcm5hdGUubTN1OA==

* Construct playout URL with German Subtitles:
http://127.0.0.1:8500/eos/v1/hls/vod/translate/en-US/aHR0cDovL3NhbXBsZS52b2RvYm94LmNvbS9wbGFuZXRlX2ludGVyZGl0ZS9wbGFuZXRlX2ludGVyZGl0ZV9hbHRlcm5hdGUubTN1OA==/eos_manifest.m3u8?languages=de-DE&default=de-DE

* * *

## HTTPS Server and Caching
This package is using python HTTP server.
To make it suitable for production and enable HTTPS + caching for subtitle fragments, you can use Nginx with reverese proxy as the outbound HTTPS server, and enable content caching.

