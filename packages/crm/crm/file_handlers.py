import datetime
import enum
from uuid import UUID
from pathlib import Path
import re
from loguru import logger

import pandas as pd

from arbm_core.private import Session
from arbm_core.private.linkedin import LinkedinCompany, LinkedinUrl

from parsing.loaders.loader_investors import load_investors
from parsing.loaders.loader_linkedin import LinkedinImportError, load_activity, load_likers

import util
from util import utc_now


import requests
from PIL import Image, UnidentifiedImageError


THUMBNAIL_SIZE = 128, 128
THUMBNAILS_PATH = util.data_path() / 'thumbnails'

THUMBNAILS_PATH.mkdir(parents=True, exist_ok=True)


def save_image(path, filename, url):
    response = requests.get(url, stream=True).raw

    save_path = THUMBNAILS_PATH / path
    if not save_path.exists():
        save_path.mkdir(parents=True, exist_ok=True)

    try:
        with Image.open(response) as im:
            im.thumbnail(THUMBNAIL_SIZE, resample=Image.BICUBIC)
            im = im.convert('RGB')
            im.save(save_path / filename, "JPEG")
    except UnidentifiedImageError as e:
        logger.error(f"Could not save image {url} to {save_path / filename}: {e}")


def save_project_thumbnail(project_uuid: UUID, url: str) -> Path:
    path = Path('projects')
    filename=f"{project_uuid}.jpg"
    save_image(path=path, filename=filename, url=url)
    return path / filename

def save_fund_thumbnail(fund_uuid: UUID, url: str) -> Path:
    path = Path('funds')
    filename = f"{fund_uuid}.jpg"
    save_image(path=path, filename=filename, url=url)
    return path / filename


class UploadFileType(enum.Enum):
    linkedin_investor_list = "linkedin_investors"
    linkedin_investor_activity = "linkedin_investor_activity"
    linkedin_post_likers = "linkedin_post_likers"
    linkedin_projects = "projects_identified"


def upload_investors(file):
    handle_file_upload(UploadFileType.linkedin_investor_list, file)


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ("http", "https") and ref_url.netloc == test_url.netloc


ALLOWED_EXTENSIONS = {"csv"}


def file_allowed(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# todo: fix file uploads
def handle_file_upload(file_type: UploadFileType, file):
    upload_folder = util.data_path() / "uploads" / "linkedin-uploads"
    handle = None

    if not(file_allowed(file.filename)):
        raise ValueError("filetype not supported")

    match file_type:
        case UploadFileType.linkedin_investor_list:
            upload_folder /= "investors"
            handle = handle_linkedin_investors
        case UploadFileType.linkedin_investor_activity:
            upload_folder /= "activity"
            handle = handle_linkedin_activity
        case UploadFileType.linkedin_post_likers:
            upload_folder /= "post_likes"
            handle = handle_linkedin_likers
        case UploadFileType.linkedin_projects:
            upload_folder /= "projects_identified"
            handle = linkedin_update_project_data
        case _:
            raise ValueError("filetype not supported")

    upload_folder /= datetime.date.today().strftime("%Y-%m-%d")

    upload_folder.mkdir(parents=True, exist_ok=True)

    filename_clean = re.sub(r"[/\\?%*:|\"<>\x7F\x00-\x1F]", "-", file.filename)

    filename, ext = filename_clean.split('.')
    filename = f'{filename}-{utc_now().strftime("%Y-%m-%d %H:%M")}.{ext}'
    filepath = upload_folder / filename

    file_contents = file.file
    with open(filepath, 'wb') as destination:
        destination.write(file_contents.read())

    if handle is None:
        raise ValueError("Handle is not implement for this filetype")

    handle(filepath)
    filepath.unlink()


def handle_linkedin_investors(filepath: Path):
    n_uploaded, n_updated = load_investors(pd.read_csv(filepath))
    return f"{n_uploaded} investors loaded and {n_updated} updated"


def handle_linkedin_activity(filepath: Path):
    try:
        loader_stats = load_activity(pd.read_csv(filepath))
        return f"{loader_stats['activity_loaded']} activity items loaded," \
                f" updated: {len(loader_stats['activity_updated'])} activity," \
                f" {len(loader_stats['posts_updated'])} posts"

    except LinkedinImportError as e:
        return "Investor(s) not found:<br>" + "<br>".join(e.errors["profiles"])


def handle_linkedin_likers(filepath: Path):
    n = load_likers(pd.read_csv(filepath))
    return f"{n} post like items loaded"


def linkedin_update_project_data(filepath: Path, overwrite_url: bool = False):
    """
    Update LinkedIn projects with user-supplied company LinkedIn URLs
    :param overwrite_url: should the URL be overwritten if already exists
    :param filepath: path to the uploaded file
    """
    with Session() as s:
        # latest_f, date = util.get_latest_file(
        #     util.get_output_dirs_by_date(app.config["UPLOAD_FOLDER"] / "linkedin-uploads/activity")[0][0],
        #     filename_pattern=file_type
        # )
        # print(f"found latest file: {latest_f} from {date}")

        df = pd.read_csv(filepath)

        for i, p in df.iterrows():
            if "company_url" in p and p["company_url"]:
                company_profile = (
                    s.query(LinkedinCompany)
                    .where(LinkedinCompany.urls.any(LinkedinUrl.url == p["company_url"]))
                    .one_or_none()
                )

                # if url is already present, and override_url=False, skip company
                if overwrite_url and company_profile is not None and len(company_profile.urls) > 0:
                    continue

                if not company_profile:
                    company_profile = LinkedinCompany(name=p["project_name"])
                    company_profile.urls.append(LinkedinUrl(url=p["company_url"]))

                s.add(company_profile)
                s.commit()
