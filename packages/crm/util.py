import asyncio
import datetime
from enum import Enum
import hashlib
import json
import re
import secrets
import string
import time
import urllib.request
import uuid
from collections import OrderedDict
from functools import reduce
from pathlib import Path
from typing import Callable, Optional, TypeVar
from urllib.parse import urlparse
from contextlib import contextmanager
from timeit import default_timer

import aiofiles
import aiohttp
from loguru import logger
import pandas as pd
import pytz
from passlib.context import CryptContext
from PIL import Image
from pydantic import BaseModel
import requests

from sqlalchemy.orm import Session

from arbm_core.private.logging import LogEntry


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


TIMEZONE = pytz.UTC
SOD = datetime.datetime.now(tz=TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
EOD = SOD + datetime.timedelta(hours=24)


def project_root() -> Path:
    return Path(__file__).parent


def discovery_twitter_path() -> Path:
    return project_root() / "data/discovery-twitter"


def data_path() -> Path:
    return project_root() / "data"


class Weekday(Enum):
    monday = 1
    tuesday = 2
    wednesday = 3
    thursday = 4
    friday = 5
    saturday = 6
    sunday = 7

    mon = monday
    tue = tuesday
    wed = wednesday
    thu = thursday
    fri = friday
    sat = saturday
    sun = sunday


RE_PARTIAL_PROTOCOL = r'^(?:(?:https?:\/\/)?(?:[\/:]*www\.)?)'

LINKEDIN_ID_ALLOWED_CHARS = r'[-a-zA-Z0-9()@:%_\+.~#?&=\']'

# match any linkedin url with full protocol
RE_LINKEDIN_URL = r'https:\/\/(www\.)[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}' \
                  r'\b\/?([-a-zA-Z0-9()@:%_\+.~#?&=\']+\/?){2}'


# match linkedin profile url of the form linkedin.com/in/username/
RE_LINKEDIN_PROFILE_URL = r'https:\/\/(www\.)[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}' \
                          r'\/in\/([-a-zA-Z0-9()@:%_\+.~#?&=\']+\/?){1}'

HR = '\n' + ('_' * 80) + '\n\n'


THUMBNAIL_SIZE = 128, 128
THUMBNAILS_PATH = data_path() / 'thumbnails'

THUMBNAILS_PATH.mkdir(parents=True, exist_ok=True)


class UrlParsingError(Exception):
    pass


def download_image(projet_uuid: uuid.UUID, url: str):
    response = requests.get(url, stream=True).raw

    with Image.open(response) as im:
        im.thumbnail(THUMBNAIL_SIZE, resample=Image.BICUBIC)
        im.save(THUMBNAILS_PATH / f"{projet_uuid}.jpg", "JPEG")


def clean_url(url: str) -> str:
    parsed_url = urlparse(url)
    updated_url = parsed_url._replace(query=None)

    cleaned_url = updated_url.geturl()
    cleaned_url = cleaned_url.strip().strip('/')
    return cleaned_url



def remove_protocol(url: str) -> str:
    naked_url = re.sub(rf'^{RE_PARTIAL_PROTOCOL}?', '', url)
    return naked_url


def strip_url(url: str) -> str:
    """
    Remove protocol, www. and query arguments from url, make lowercase
    """
    return remove_protocol(clean_url(url)).lower()


def get_url_root(v: str) -> str:
    """
    Return url root, e.g. https://www.google.com/page -> https://www.google.com
    """
    if v is None:
        return v

    url = urlparse(strip_url(v))
    updated_url = url._replace(path=None, fragment=None)

    return updated_url.geturl()



def prune_website(v: str) -> str:
    '''
    Prepares website url for pydantic validation
    by ensuring https:// is present and url is cleaned
    returns None if v is None
    '''
    if v is None:
        return v

    return f"https://{strip_url(v)}"


def hash_password(password):
    return pwd_context.hash(password)


def generate_password() -> (str, str):
    symbols = ",./<>?!@#$%&*(){}"
    alphabet = string.ascii_letters + string.digits + symbols
    while True:
        password = ''.join(secrets.choice(alphabet) for i in range(10))
        if (any(c.islower() for c in password)
                and any(c.isupper() for c in password)
                and sum(c.isdigit() for c in password) >= 3
                and sum([c in symbols for c in password]) >= 2):
            break

    return password, hash_password(password)


def nested_get(dictionary: dict, *keys: str, default=None, handle_list=True):
    """
    Get value from nested dictionary
    :param dictionary: nested dictionary
    :param keys: keys to traverse
    :param default: default value to return if key not found
    :param handle_list: if True, will return first element of list if value is a list
    """
    if handle_list:
        return reduce(lambda d, key:
                        d.get(key) if isinstance(d, dict) else (
                            d[0] if isinstance(d, list) and len(d) else default
                        ),
                      keys, dictionary)

    return reduce(lambda d, key: d.get(key) if isinstance(d, dict) else default, keys, dictionary)


def get_linkedin_id(url: str, *, profile_type: str = 'personal') -> str:
    pattern = None

    match profile_type:
        case 'personal':
            pattern = rf'^{RE_PARTIAL_PROTOCOL}?linkedin.com\/in\/' \
                        f'({LINKEDIN_ID_ALLOWED_CHARS}+)\/?'
        case 'company':
            pattern = rf'^{RE_PARTIAL_PROTOCOL}?linkedin.com\/company\/' \
                        f'({LINKEDIN_ID_ALLOWED_CHARS}+)\/?'
        case _:
            raise ValueError("linkedin profile type must be one of personal | company")

    match = re.search(pattern, url)

    logger.debug(f'linkedin url {url} yilded {"id " + match.group(1) if match else "nothing"}')

    if match:
        linkedin_id = match.group(1)
        return linkedin_id

    raise UrlParsingError(f'Could not parse linkedin id from url {url}')


def validate_linkedin_url(url: str):
    if not url:
        raise ValueError(f'Linkedin Url "{url}" is empty or None')

    url_full_protocol = re.sub(RE_PARTIAL_PROTOCOL, 'https://www.', clean_url(url), count=1)

    if not re.match(RE_LINKEDIN_URL, url_full_protocol):
        raise ValueError(f'Linkedin Url {url_full_protocol} is not valid, make sure it starts with https://www.')

    return url_full_protocol


def validate_linkedin_profile_url(url):
    url_full_protocol = validate_linkedin_url(url)

    if not re.match(RE_LINKEDIN_PROFILE_URL, url_full_protocol):
        raise ValueError(f'Linkedin Url {url_full_protocol} does not seem to be a valid profile URL')

    return url_full_protocol


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, (uuid.UUID)):
        return str(obj)
    if isinstance(obj, BaseModel):
        return obj.dict()
    if isinstance(obj, Session):
        return 'db object'
    raise TypeError ("Type %s not serializable" % type(obj))


def log_event(s, **kwargs):
    new_kwargs = kwargs.copy()

    if message_dict := kwargs.get('message'):
        new_kwargs['message'] = json.dumps(message_dict, default=json_serial)

    event = LogEntry(**new_kwargs)
    s.add(event)
    s.commit()


def db_path() -> Path:
    return data_path() / "db"


def read_const_file(filename: Path | str) -> Path:
    return Path(project_root(), "const", filename)


def read_list(const_filename: Path | str):
    return [v for v in (read_const_file(const_filename).read_text().strip().split("\n")) if v]


def read_dict(const_filename, cast_value: Optional[Callable] = None):
    entries = [line.split() for line in read_list(const_filename)]

    return {key: (cast_value(val) if cast_value else val) for key, val in entries}


def download_file(file_url: str, filepath: Path, filename: str) -> Path:
    """
    Download file from remote URl
    :return: Path to the created file
    """
    filepath.mkdir(parents=True, exist_ok=True)
    filepath = filepath / filename
    urllib.request.urlretrieve(file_url, filepath)
    return filepath


async def download_file_async(file_url: str, filepath: Path) -> Path:
    """
    Download file from remote URl asynchronously
    :return: Path to the created file
    """
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get(file_url) as resp:
            if resp.status == 200:
                async with aiofiles.open(filepath, mode="wb") as f:
                    await f.write(await resp.read())
        return filepath


def get_files(directory: Path):
    if not directory.is_dir():
        raise ValueError("Path to directory expected")

    files = []
    for f in directory.iterdir():
        if f.is_file():
            files.append(f)

    return files


def get_folders(directory: Path):
    if not directory.is_dir():
        raise ValueError("Path to directory expected")

    for d in directory.iterdir():
        if d.is_dir():
            yield d


def get_files_by_date(folder: Path, filename_pattern: Optional[str] = None):
    files_by_date = []
    for f in get_files(folder):
        if filename_pattern and not re.search(filename_pattern, f.name):
            continue

        f_date = parse_datetime(f.name)
        files_by_date.append((f, f_date))

    return sorted(files_by_date, key=lambda f: f[1], reverse=True)


def get_latest_file(folder: Path, filename_pattern: Optional[str] = None) -> (Path, datetime.datetime):
    """
    :param folder: path where to search for files
    :param filename_pattern: if provided, file must contain in it's filename
    :return: file with latest date in the file name
    """
    max_date = datetime.datetime.fromtimestamp(0)
    f_latest = None

    for f, f_date in get_files_by_date(folder, filename_pattern):
        if f_date > max_date:
            max_date = f_date
            f_latest = f

    return f_latest, max_date


def parse_datetime(text) -> Optional[datetime.datetime]:
    date = re.search(r"(\d{4})-(\d{2})-(\d{2})\s*(?:(\d{2}):(\d{2})(?::(\d{2})(?:.(\d{1,6}))?)?)?", text)
    if date:
        return datetime.datetime(*[int(x) for x in date.groups() if x])


def file_hash(filepath: Path):
    with open(filepath, "rb") as f:
        hash_f = hashlib.blake2b()
        while chunk := f.read(8192):
            hash_f.update(chunk)
        return hash_f.hexdigest()


T = TypeVar("T", str, int)


def bucket_value(buckets: dict[T, int], val: int) -> T:
    buckets_sorted = OrderedDict(sorted(buckets.items(), key=lambda pair: pair[1], reverse=True))

    for bucket, min_val in buckets_sorted.items():
        val = 0 if pd.isna(val) else val
        if val >= min_val:
            return bucket

    raise ValueError(f"Invalid bucket mapping provided, value ({val}) " f"does not fall under any bucket: {buckets}")


def score_text(words_scores: dict[str, int], text: str) -> int:
    total_score = 0
    for word, word_score in words_scores.items():
        if re.search(word, text):
            total_score += word_score
    return total_score


async def resolve_url(url):
    timeout = 30
    longurl = None
    status = None

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=timeout) as r:
                # print(f'r url: {r.url} (status {r.status})')
                status = r.status
                longurl = r.url
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            pass

    return url, longurl, status


async def expand_urls_native(urls):
    start = time.time()

    resolved_urls = await asyncio.gather(*[resolve_url(url) for url in urls])

    # all_resolved = list(zip(*resolved_urls))
    # all_urls = pd.DataFrame.from_dict({'url': all_resolved[0], 'longurl': all_resolved[1], 'status': all_resolved[2]})
    # all_urls['status'] = all_urls.status.astype('category')

    end = time.time()
    elapsed = round(end - start, 2)  # 10th of second precision
    logging.debug(
        f"took {elapsed}s to resolve {len(resolved_urls)} uls,"
        f"{len(resolved_urls) / max(elapsed, 1)} url/s"
    )

    resolved = list(zip(*resolved_urls))
    return resolved[1] if len(resolved) > 1 else []


async def get_website_html(startups: pd.DataFrame, website_col) -> pd.DataFrame:
    async def extract_website_html(df, url):
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url)
            content = await resp.text()
            df.loc[df[website_col] == url, [f"{website_col}_html"]] = [[content]]

    df = startups.copy()
    await asyncio.gather(*[extract_website_html(df, url) for url in df[website_col].unique()], return_exceptions=True)
    return df


def twitter_url_to_username(url):
    p = r"com?/(\w+).*"
    match = re.search(p, url)
    return match.group(1).lower() if match else None


def urls_to_usernames(urls: pd.Series) -> pd.Series:
    """
    Return series of usernames extracted from the url, that follow .co(m)/
    :param urls:
    :return:
    """
    urls.fillna("", inplace=True)
    urls = urls.apply(twitter_url_to_username)
    return urls


def resolve_merge(merged, resolve_merge="", override: dict[str, str] = {}, debug=False):
    """
    Given two dataframes, add the columns from right dataframe to the left dataframe if not present yet,
    otherwise take the non-null column out of two, giving preference to the right columns
    :param left, right: dataframes with potentially clashing columns
    :return: merged dataframe
    """
    valid_resolves = ["always_x", "always_y", "prioritise_y"]
    if resolve_merge not in valid_resolves:
        raise ValueError(
            f'"resolve_merge" parameter value ({resolve_merge}) is not valid,' f"must be one of {valid_resolves}"
        )

    for col, x_or_y in override.items():
        merged[col] = merged[f"{col}_{x_or_y}"]

    for col in [col for col in merged.columns if col[-2:] == "_y"]:
        col = col[:-2]
        if debug:
            print("mergin col:", col)

        y_col = f"{col}_y"
        x_col = f"{col}_x"

        if resolve_merge == "prioritise_y":
            merged[col] = merged.apply(
                lambda x: x[y_col]
                if pd.notna(x[y_col]) and (not isinstance(x[y_col], str) or len(x[y_col]) > 0)
                else x[x_col],
                axis=1,
            )
        elif resolve_merge == "always_x":
            merged[col] = merged[x_col]
        elif resolve_merge == "always_y":
            merged[col] = merged[y_col]

        if debug:
            print(merged[[x_col, y_col, col]])

        if f"{col}_y" in merged.columns:
            merged = merged.drop(f"{col}_y", axis=1)
        if f"{col}_x" in merged.columns:
            merged = merged.drop(f"{col}_x", axis=1)

    return merged


def update_and_append(file, new_output, index_col):
    saved = pd.read_csv(file, index_col=index_col)
    saved = pd.concat([new_output, saved[~saved.index.isin(new_output.index)]])
    saved.to_csv(file, index=True)


def round_sf(x, sf=3):
    if isinstance(x, float):
        return round(x, sf)
    return x


def get_output_dirs_by_date(folder="out") -> list[tuple[Path, datetime.date]]:
    """
    Return list of folders that contain historical outputs along with their date,
    starting from latest
    :return:
    """
    p_date = r"(\d\d\d\d)-(\d\d)-(\d\d)"

    dirs = []
    for dir in get_folders(data_path() / folder):
        date_match = re.search(p_date, dir.name)
        if date_match:
            dir_date = datetime.date(*[int(x) for x in date_match.groups()[0:3]])  # get data date from folder name
            dirs.append((dir, dir_date))

    dirs = sorted(dirs, key=lambda x: x[1], reverse=True)
    return dirs


def get_previous_outputs(
    filename_pattern="raw-output", exclude_today=True, folder="out"
) -> list[tuple[datetime.date, Path]]:
    """
    For each day of outputs (dated folder), return the latest file by date in filname and it's folder date.
    :param filename_pattern: filter files by having the given pattern in the filename
    :param exclude_today: whether to include the latest output from today
    :return:
    """
    # load all output files with an associated date, except for today's output
    historic_outputs = []

    for dir, dir_date in get_output_dirs_by_date(folder):
        if exclude_today and dir_date == datetime.date.today():
            continue

        f, _ = get_latest_file(dir, filename_pattern=filename_pattern)

        if not f:
            logging.warning(f"output not found for dir {dir}")
            continue

        historic_outputs.append((dir_date, f))

    return historic_outputs



def gen_uiid(*args):
    uid = uuid.uuid4()
    return uid


def print_json(s):
    print(json.dumps(s, indent=4, sort_keys=True))


def rename_if_possible(df: pd.DataFrame, mapping: dict[str, str]) -> None:
    new_mapping = {}
    for old_col, new_col in mapping.items():
        if old_col in df.columns:
            new_mapping.update({old_col: new_col})
    df.rename(new_mapping, axis=1, inplace=True)


def utc_now():
    return datetime.datetime.now(pytz.UTC)


def now_fmt(fmt: str):
    return utc_now.strftime(fmt)


def dt_fmt(time: datetime.datetime, seconds=False) -> str:
    fmt = "%a, %-d/%m %H:%M{seconds} %Z".format(seconds=":%S" if seconds else "")
    return time.strftime(fmt)


def get_today() -> str:
    return now_fmt("%Y-%m-%d")


@contextmanager
def elapsed_timer():
    start = default_timer()
    elapser = lambda: default_timer() - start
    yield lambda: elapser()
    end = default_timer()
    elapser = lambda: end-start