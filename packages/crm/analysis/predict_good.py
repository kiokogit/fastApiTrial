import datetime
import pickle

import pandas as pd
from arbm_core.private import Session
from arbm_core.private.projects import ProjectAnalytics, TrackedProject
from sklearn.preprocessing import OrdinalEncoder

import util

ORD_ENCODER_PATH = util.project_root() / 'const/ml_models' / 'ordinal_encoder_generic.sav'
ORD_ENCODER_STAGE_PATH = util.project_root() / 'const/ml_models' / 'ordinal_encoder_investment_stage.sav'
LR_MODEL_PATH = util.project_root() / 'const/ml_models' / 'logistic_model_predict_good.sav'


def location_to_country(location):
    if not location:
        return "Unknown"
    print(f'extracting country from location {location}')
    country = location.split(',')[-1].strip()
    print(f'converted location {location} into country {country}')
    return country


def last_round_to_year(last_round):
    if not last_round or pd.isna(last_round):
        return 0
    if isinstance(last_round, datetime.datetime):
        return last_round.year
    if isinstance(last_round, datetime.date):
        return last_round.year
    print(type(last_round), last_round)
    return datetime.datetime.strptime(last_round, "%Y-%m-%d").year


def prepare_data(project: TrackedProject):
    analytics: ProjectAnalytics = project.analytics

    if analytics is None:
        raise ValueError("project analytics does not exist")

    project_x = pd.DataFrame.from_records([{
        'country': location_to_country(analytics.location),
        'team_size': analytics.team_size,
        'founded': analytics.founded,
        'stage': analytics.stage.value if analytics.stage else None,
        'previous_exit': analytics.previous_exit,
        'funding': analytics.funding,
        'last_round': last_round_to_year(analytics.last_round),
        'last_round_amount': analytics.last_round_amount,
        'verticals': [c.name for c in analytics.get_attr('verticals', 'tag')],
    }])

    median_cols = ["team_size", "founded", "funding", "last_round_amount"]
    medians = {
        "team_size": 14.25,
        "founded": 2019,
        "funding": 2500,
        "last_round_amount": 2000
    }

    project_x[median_cols] = project_x[median_cols].fillna(medians)
    project_x["stage"] = project_x["stage"].fillna('Seed')

    project_x.dropna(inplace=True)

    one_hot_cols = ["country", "previous_exit"]

    with open(ORD_ENCODER_PATH, 'rb') as pickle_file:
        ord_encoder: OrdinalEncoder = pickle.load(pickle_file)
    with open(ORD_ENCODER_STAGE_PATH, 'rb') as pickle_file:
        ord_encoder_stage: OrdinalEncoder = pickle.load(pickle_file)

    project_x[one_hot_cols] = ord_encoder.transform(
        project_x[one_hot_cols]
    )
    project_x[['stage']] = ord_encoder_stage.transform(
        project_x[['stage']]
    )

    # one-hot encode verticals
    vertical_values = pd.read_csv(util.project_root() / "const/ml_models/analytics_categories.csv")
    vertical_values = set(vertical_values["name"].to_list())

    def encode_verticals(verticals, values: list) -> list:
        return [1 if f in verticals else 0 for f in values]

    encoded_vert = project_x["verticals"].apply(encode_verticals, args=(vertical_values,))

    df_verts = pd.DataFrame(encoded_vert.tolist(), columns=list(vertical_values))
    df_verts.reset_index(drop=True, inplace=True)
    project_x.reset_index(drop=True, inplace=True)
    project_x = pd.concat([project_x, df_verts], axis=1)
    project_x.drop("verticals", axis=1, inplace=True)

    return project_x


def predict_good(project: pd.DataFrame):
    with open(LR_MODEL_PATH, 'rb') as pickle_file:
        model = pickle.load(pickle_file)

    cols_when_model_builds = model.feature_names_in_
    project = project[cols_when_model_builds]

    pred_label = model.predict(project)
    pred_proba = model.predict_proba(project)

    return int(pred_label[0]), pred_proba[0].astype(float)


if __name__ == '__main__':
    with Session() as s:
        p = s.get(TrackedProject, 21752)
        p_x = prepare_data(p)

        # for col in p_x.columns:
        #     print(f'{col}: {p_x.loc[0][col]}')

        pred_label, pred_proba = predict_good(p_x)
        print(pred_label, pred_proba)

