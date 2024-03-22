from html.parser import HTMLParser
from io import TextIOWrapper
from typing import BinaryIO, ClassVar, List

from dl_plus import ytdl
from dl_plus.extractor import Extractor, ExtractorError, ExtractorPlugin


[
    clean_html, int_or_none, parse_iso8601,
    determine_ext, urlhandle_detect_ext,
] = ytdl.import_from('utils', [
    'clean_html', 'int_or_none', 'parse_iso8601',
    'determine_ext', 'urlhandle_detect_ext',
])


__version__ = '0.2.0'


plugin = ExtractorPlugin(__name__)


class _NextJSHTMLParser(HTMLParser):
    _fobj: BinaryIO
    _finished: bool
    _handling_next_data: bool
    _data_parts: List[str]

    _CHUNK_SIZE: ClassVar[int] = 1000

    def __init__(self, fobj: BinaryIO) -> None:
        super().__init__(convert_charrefs=False)
        self._fobj = fobj
        self._finished = False
        self._handling_next_data = False
        self._data_parts = []

    def parse(self) -> bool:
        assert not self._finished
        with TextIOWrapper(self._fobj, encoding='utf-8') as wrapper:
            while not self._finished:
                chunk = wrapper.read(self._CHUNK_SIZE)
                if not chunk:
                    break
                self.feed(chunk)
        self._handling_next_data = False
        success = self._finished
        self._finished = True
        return success

    def get_data(self) -> str:
        assert self._finished
        return ''.join(self._data_parts)

    def handle_starttag(self, tag, attrs):
        if self._finished:
            return
        assert not self._handling_next_data, f'unexpected nested tag: {tag}'
        if tag == 'script' and dict(attrs).get('id') == '__NEXT_DATA__':
            self._handling_next_data = True

    def handle_endtag(self, tag):
        if not self._handling_next_data:
            return
        assert tag == 'script', f'unexpected closing tag: {tag}'
        self._handling_next_data = False
        self._finished = True

    def handle_data(self, data):
        if not self._handling_next_data:
            return
        self._data_parts.append(data)


class _RinseFMBaseExtractor(Extractor):
    DLP_BASE_URL = r'https?://(?:www\.)?rinse\.fm'

    def _get_slug(self, url) -> str:
        return self._match_valid_url(url).group('slug')

    def _fetch_entry_data(self, url, slug) -> dict:
        response = self._request_webpage(url, video_id=slug)
        parser = _NextJSHTMLParser(response)
        if not parser.parse():
            raise ExtractorError('missing or incomplete data', video_id=slug)
        root_data = self._parse_json(parser.get_data(), video_id=slug)
        return root_data['props']['pageProps']['entry']

    def _fetch_formats(self, url, slug) -> List[dict]:
        ext = None
        filesize = None
        # HEAD requests won't work
        response = self._request_webpage(url, video_id=slug, fatal=False)
        if not response:
            raise ExtractorError(
                'Probably dead link', video_id=slug, expected=True)
        format_url = response.url
        ext = urlhandle_detect_ext(response)
        if not ext:
            ext = determine_ext(format_url)
        filesize = int_or_none(response.headers.get('Content-Length'))
        return [{
            'url': format_url,
            'ext': ext,
            'filesize': filesize,
            'vcodec': 'none',
        }]


@plugin.register('channel')
class RinseFMChannelExtractor(_RinseFMBaseExtractor):
    DLP_REL_URL = r'/channels/(?P<slug>[^/#?]+)'

    def _real_extract(self, url):
        slug = self._get_slug(url)
        entry_data = self._fetch_entry_data(url, slug)
        stream_url = entry_data.get('streamerMountPoint')
        if not stream_url:
            raise ExtractorError('no streamerMountPoint', video_id=slug)
        return {
            'id': entry_data.get('id', slug),
            'display_id': slug,
            'title': entry_data.get('title', slug),
            'description': clean_html(entry_data.get('description')),
            'formats': self._fetch_formats(stream_url, slug),
            'is_live': True,
        }


@plugin.register('episode')
class RinseFMEpisodeExtractor(_RinseFMBaseExtractor):
    DLP_REL_URL = r'/episodes/(?P<slug>[^/#?]+)'

    def _real_extract(self, url):
        slug = self._get_slug(url)
        entry_data = self._fetch_entry_data(url, slug)
        file_url = entry_data.get('fileUrl')
        if not file_url:
            # not all episodes have replays -> extected=True
            raise ExtractorError('no fileUrl', video_id=slug, expected=True)
        info_dict = {
            'id': entry_data.get('id', slug),
            'display_id': slug,
            'title': entry_data.get('title', slug),
            'description': clean_html(entry_data.get('description')),
            'formats': self._fetch_formats(file_url, slug),
        }
        # episode datetime is split to two fields:
        # * episodeDate contains only date (time is 00:00:00)
        # * episodeTime contains only time (date is today)
        episode_date = entry_data.get('episodeDate')
        if episode_date:
            episode_time = entry_data.get('episodeTime')
            if episode_time:
                _date, _, _ = episode_date.partition('T')
                _, _, _time = episode_time.partition('T')
                episode_datetime = f'{_date}T{_time}'
            else:
                episode_datetime = episode_date
            info_dict['release_timestamp'] = parse_iso8601(episode_datetime)
        duration = entry_data.get('episodeLength')
        if duration:
            info_dict['duration'] = duration * 60
        genre_data = entry_data.get('genreTag')
        if genre_data:
            info_dict['genre'] = ', '.join(
                genre['title'] for genre in genre_data)
        return info_dict
