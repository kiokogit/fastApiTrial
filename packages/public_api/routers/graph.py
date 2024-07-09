from collections import defaultdict
import datetime
import itertools
from pprint import pprint, pformat

from loguru import logger
from fastapi import APIRouter, Request
import numpy as np
from pydantic import ValidationError
from pydantic.color import Color
from sqlalchemy.orm import Session

from arbm_core.private.projects import TrackedProject
from arbm_core.public.projects import Project, FundProfile
from arbm_core.public.schemas.graph import Graph, Node, Link
from dependencies import PrivateSession, LoggedInUser

from search_utils import search_and_publish
from schemas.schemas import SearchFiltersSchema
from schemas.funds import FundSchema
from schemas.feed import ProjectEntry, ProjectSchema

from utils import log_user_event,\
                  get_user_feed_projects, color_scaler


PROJECT_MIN_YEAR = 2015
PROJECT_MAX_YEAR = datetime.date.today().year


PROJECTS_GROUP = 1
UNISSUED_PROJECTS_GROUP = 3
FUNDS_GROUP = 2
UNISSUED_FUNDS_GORUP = 4

PROJECTS_COLOR = Color('hsl(181, 100%, 14%, .9)')
UNISSUED_PROJECTS_COLOR = Color('hsl(181, 0%, 48%, .8)')
FUNDS_COLOR = Color('hsl(5, 73%, 64%, .9)')
UNISSUED_FUNDS_COLOR = Color('hsl(297, 70%, 30%, .8)')

CLUSTER_COLORS = [
    'red', 'yellow', 'blue', 'green', 'purple', 'orange', 'pink', 'teal'
]

router = APIRouter()


def compute_graph(user_projects: list[dict], projects: list[Project]) -> Graph:
    nodes = set()
    links = set()

    for project_orm in projects:
        try:
            project_schema = ProjectSchema(**project_orm.__dict__) if not isinstance(project_orm, dict) else ProjectSchema(**project_orm)
        except ValidationError:
                logger.error(f'error validating project {project_orm}:'\
                             f'\n\n{pformat(project_orm)}')
                continue

        project_funds = project_schema.funds if project_schema.funds else []
        project_user_data = None
        comments = None

        user_project_uuids = [p['project'].uuid for p in user_projects]
        if project_idx := (user_project_uuids.index(project_schema.uuid) if project_schema.uuid in user_project_uuids else None):
            project_user_data = user_projects[project_idx]['project_user_info']
            comments = user_projects[project_idx]['comments']

        try:
            project_entry = ProjectEntry(**{'project': project_schema, 'project_user_info': project_user_data, 'comments': comments})
        except ValidationError:
            logger.error(f'error validating project entry for {project_orm}')
            continue

        for f in project_funds:
            nodes.add(
                Node(id=f.name, group=FUNDS_GROUP, color=FUNDS_COLOR, object_data=f)
            )

            links.add(
                Link(source=f.name, target=str(project_schema.uuid))
            )

        nodes.add(
            Node(
                id=str(project_schema.uuid),
                    group=PROJECTS_GROUP,
                    color=PROJECTS_COLOR,
                    object_data=project_entry
                )
         )


    return Graph(nodes=list(nodes), links=list(links))


def update_graph_view(public_s, user_projects: list[dict], graph: Graph):
    user_funds = set()
    for p in user_projects:
        user_funds.update(p['project'].funds)

    for node in graph.nodes:
        if node.group == PROJECTS_GROUP:
            project_data: ProjectEntry = node.object_data
            project_orm: Project = public_s.query(Project).get(project_data.project.uuid)

            if project_data.project.uuid not in [p['project'].uuid for p in user_projects]:
                node.group = UNISSUED_PROJECTS_GROUP
                node.color = UNISSUED_PROJECTS_COLOR

            hsl_tuple = node.color.as_hsl_tuple(alpha=True)

            year_founded_val: str | None = None
            try:
                year_founded_val = project_orm.founded
            except AttributeError:
                continue

            year_founded: int = int(year_founded_val) if year_founded_val and year_founded_val.isnumeric() else PROJECT_MIN_YEAR

            if node.group == UNISSUED_PROJECTS_GROUP:
                alpha_scaler = color_scaler(PROJECT_MIN_YEAR, PROJECT_MAX_YEAR, exponential=True)
                alpha = alpha_scaler(hsl_tuple[3], 0.4, year_founded)
                new_hsl = (hsl_tuple[0] * 360, hsl_tuple[1] * 100, hsl_tuple[2] * 100, alpha)
            else:
                saturation_scaler = color_scaler(PROJECT_MIN_YEAR, PROJECT_MAX_YEAR, exponential=True)
                lightness_scaler = color_scaler(PROJECT_MIN_YEAR, PROJECT_MAX_YEAR)

                saturation = saturation_scaler(hsl_tuple[1], min(0.16, hsl_tuple[1]), year_founded)
                lightness = 1 - lightness_scaler(0.90, 0.30, year_founded)

                alpha = saturation_scaler(hsl_tuple[3], 0.3, year_founded)

                new_hsl = (hsl_tuple[0] * 360, saturation * 100, lightness * 100, hsl_tuple[3])

            # logger.debug(f"old color val: hsl({hsl_tuple[0]}, {hsl_tuple[1]}%, {hsl_tuple[2]}%, {hsl_tuple[3]})")
            # logger.debug(f"new color val: hsl({new_hsl[0]}, {new_hsl[1]}%, {new_hsl[2]}%, {new_hsl[3]})\n")

            node.color = Color(f"hsl({new_hsl[0]}, {new_hsl[1]}%, {new_hsl[2]}%, {new_hsl[3]})")

        elif node.group == FUNDS_GROUP:
            fund_data: FundSchema = node.object_data

            if fund_data.name not in [f.name for f in user_funds]:
                node.group = UNISSUED_FUNDS_GORUP
                node.color = UNISSUED_FUNDS_COLOR

            recent_signal = False
            fund: FundProfile = public_s.query(FundProfile).filter(FundProfile.name==fund_data.name).one()

            for p in fund.projects:
                signal_date = p.discovered_date.date() if isinstance(p.discovered_date, datetime.datetime) else p.discovered_date

                if signal_date > (datetime.datetime.now() - datetime.timedelta(days=7)).date():
                    recent_signal = True
                    break

            if not recent_signal:
                old_hsl = node.color.as_hsl_tuple(alpha=True)
                saturation = old_hsl[1] / 2
                lightness = old_hsl[2] / 2

                dimmed_hsl = (old_hsl[0] * 360, saturation * 100, lightness * 100, old_hsl[3])
                node.color = Color(f"hsl({dimmed_hsl[0]}, {dimmed_hsl[1]}%, {dimmed_hsl[2]}%, {dimmed_hsl[3]})")

    return graph


def graph_apply_clustering(private_s, graph: Graph):
    projects = {str(p.uuid): p for p in private_s.query(TrackedProject)
                .filter(TrackedProject.uuid.in_([node.id for node in graph.nodes if node.group in [1, 3]])).all()}

    clusters = defaultdict(list)

    for p in projects.values():
        cluster_id = p.analytics.get_attr('cluster_id', 'detail')
        if cluster_id is not None:
            clusters[cluster_id.value].append(p)

    projects_to_cluster = {}
    for cid, group in clusters.items():
        for p in group:
            projects_to_cluster[str(p.uuid)] = cid


    new_nodes = []

    for node in graph.nodes:
        if node.id in projects_to_cluster:
            # print(f'cluster for project {node.id} is {projects_to_cluster[node.id]}')
            # print(f'color: {CLUSTER_COLORS[int(projects_to_cluster[node.id])]}')
            # print(type(node.id), node.id)
            cid = projects_to_cluster[node.id]

            cluster_distances = [float(p.analytics.get_attr('distance_to_cluster_centroid', 'detail').value) for p in clusters[cid]]
            bounds = min(cluster_distances), max(cluster_distances)
            if min(cluster_distances) == max(cluster_distances):
                bounds = min(cluster_distances), min(cluster_distances) * 1.2
            color_scaler_clusters = color_scaler(*bounds, exponential=True)

            d_to_centroid = float(projects[node.id].analytics.get_attr('distance_to_cluster_centroid', 'detail').value)
            print(projects[node.id].title, d_to_centroid)

            cluster_hsl = Color(CLUSTER_COLORS[int(cid)]).as_hsl_tuple(alpha=True)
            alpha = color_scaler_clusters(cluster_hsl[3], 0.2, d_to_centroid)
            new_hsl = (cluster_hsl[0] * 360, cluster_hsl[1] * 100, cluster_hsl[2] * 100, alpha)
            node.color = Color(f"hsl({new_hsl[0]}, {new_hsl[1]}%, {new_hsl[2]}%, {new_hsl[3]})")

        new_nodes.append(node)
    graph.nodes = new_nodes

    clusters_links = []
    for cid, cluster_projects in clusters.items():
        print(f'connecting cluster {cid}')
        circle_one = []
        circle_two = []
        circle_three = []

        cluster_distances = [float(p.analytics.get_attr('distance_to_cluster_centroid', 'detail').value) for p in cluster_projects]

        p33, p66 = np.percentile(cluster_distances, [33, 66])
        print('percentiles:', p33, p66)

        for p in cluster_projects:
            print(p.uuid, cid)
            if (d_centroid := float(p.analytics.get_attr('distance_to_cluster_centroid', 'detail').value)) < p33:
                circle_one.append(str(p.uuid))
            elif d_centroid < p66:
                circle_two.append(str(p.uuid))
            else:
                circle_three.append(str(p.uuid))

        pprint(circle_one)
        # pprint(circle_two)
        # pprint(circle_three)

        # print(len(circle_one) > 2)
        dense_links = [[circle_one[-1], circle_one[0]]] if len(circle_one) > 2 else []
        dense_links.extend(list(itertools.pairwise(circle_one)))
        # pprint(dense_links)


        medium_links = list(itertools.pairwise(circle_two)) + ([[circle_two[-1], circle_two[0]]] if len(circle_two) > 2 else [])
        medium_links = list(zip(circle_two, itertools.cycle(circle_one)))

        loose_links = list(itertools.pairwise(circle_three)) + ([[circle_three[-1], circle_three[0]]] if len(circle_three) > 2 else [])
        loose_links.extend(list(zip(circle_three, itertools.cycle(circle_two))))

        cluster_links = []
        cluster_links += dense_links + medium_links + loose_links

        print(f'{len(cluster_projects)} projects, {len(cluster_links)} links in cluster {cid}')
        pprint(cluster_links)

        # for lnk in cluster_links:
            # print(str(lnk[0]), str(lnk[1]))
        clusters_links.extend([Link(source=pair[0], target=pair[1], clustering=True) for pair in cluster_links])

    graph.links.extend(clusters_links)

    print(f'total graph links {len(graph.links)}')
    pprint(graph.links)
    pprint([n.id for n in graph.nodes])

    # raise RuntimeError
    return graph


@router.get('')
def default_graph(request: Request, current_user: LoggedInUser | None) -> Graph:
    log_user_event(user=current_user,
                   event=request.url.path, details={'ip': request.client,})

    start_of_week = datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday())
    week_projects = get_user_feed_projects(current_user, query_date=start_of_week)
    user_projects = get_user_feed_projects(current_user)

    # graph = compute_graph(user=current_user, user_projects=user_projects, projects=[ProjectSchema(**e['project'].__dict__) for e in week_projects])
    graph = compute_graph(user_projects=user_projects, projects=[p for p in week_projects])

    return graph


def build_graph(project_uuids, *, current_user, public_s):
    if not project_uuids:
        return Graph(nodes=[], links=[])

    found_projects: list[Project] = public_s.query(Project).filter(Project.uuid.in_(project_uuids)).all()

    logger.debug(f"found {len(project_uuids)} projects in search and {len(found_projects)} out of these are published")

    user_projects = []

    if current_user:
        user_projects = get_user_feed_projects(current_user)

        if len(project_uuids) != len(found_projects):
            logger.critical(f"not all found projects ({len(project_uuids)}) were published ({len(found_projects)})")

    graph = compute_graph(user_projects=user_projects, projects=found_projects)
    graph = update_graph_view(public_s, user_projects, graph)

    return graph


@router.post('/search')
def search_graph(request: Request,
                 current_user: LoggedInUser,
                 private_s: PrivateSession,
                 search_filters: SearchFiltersSchema,
                 apply_clustering: bool = False) -> Graph:

    project_uuids = search_and_publish(private_s=private_s, search_filters=search_filters)

    log_user_event(user=current_user,
                    event=request.url.path,
                    details={
                            'ip': request.client,
                            'filters': search_filters.dict(),
                            'result': [str(u) for u in project_uuids],
                    })

    graph = build_graph(project_uuids, current_user=current_user, public_s=private_s)

    if apply_clustering:
        logger.debug(f'apply_clustering={apply_clustering}, running clustering on graph')
        graph = graph_apply_clustering(private_s=private_s, graph=graph)

    logger.critical('test')
    logger.debug('finished updating graph')

    issued_nodes, unissued_nodes = [n for n in graph.nodes if n.group <= 2], [n for n in graph.nodes if n.group > 2]

    logger.debug(f'found {len(issued_nodes)} issued nodes, {len(unissued_nodes)} unissued nodes')

    return graph
