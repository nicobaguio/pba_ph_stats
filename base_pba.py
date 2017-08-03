from sqlalchemy import *
import pandas as pd
import numpy as np
import copy
import os.path


def connect_hb():
    """
    Connects to HumbleBola database

    Returns:
        Engine objec of HumbleBola database
    """
    dbname = 'humblebola_stats_production'
    host = 'stats.humblebola.com'
    port = 5432
    user = 'nico'
    password = '6JzymWZM8LlTXvbvpDKF'

    connect_string = f'postgresql://{user}:{password}@{host}: \
    {port}/{dbname}'

    engine = create_engine(connect_string)

    return engine


def download(table):
    """
    Downloads a table from the HumbleBola database

    Args:
        table (str): table to download from HumbleBola database

    Returns:
        Pandas dataframe of table from HumbleBola database
    """
    engine = connect_hb()

    temp_table = pd.read_sql_table(table_name=table, con=engine)

    if table == 'game_events':
        del temp_table['lineup']
        temp_table.loc[
            (temp_table.action_type == 'substitution') &
            (temp_table.action_subtype == 'in'), 'id'] = 999998

        temp_table.loc[
            (temp_table.action_type == 'substitution') &
            (temp_table.action_subtype == 'out'), 'id'] = 999997

        temp_table.loc[
            (temp_table.action_type == 'game') &
            (temp_table.action_subtype == 'end'), 'id'] = 999999

        temp_table.loc[
            (temp_table.action_type == 'game') &
            (temp_table.action_subtype == 'start'), 'id'] = 1

        temp_table.drop(
            temp_table.index[temp_table.action_subtype == 'startperiod']
        )

        temp_table = temp_table.sort_values(
            ['game_id', 'period', 'secs_remaining', 'id'],
            ascending=[True, True, False, True])

    elif table == 'games':
        del temp_table['post_game_article_url']
        del temp_table['pre_game_article_url']
        temp_table = temp_table.sort_values('schedule')

    return temp_table


# def update(table):

#     # Create db connect
#     # hdf = initialize()
#     with pd.HDFStore('hb_db.h5') as hdf:
#         engine = connect_hb()
#         metadata = MetaData()
#         connection = engine.connect()

#         select_tbl = Table(
#             table, metadata, autoload=True, autoload_with=engine
#         )
#         select_hdf = hdf[table]
#         t_index = select_hdf.id.tolist()

#         # get updated table
#         stmt = select([select_tbl])
#         stmt = stmt.where(not_(select_tbl.columns.id.in_(t_index)))
#         results = connection.execute(stmt)
#         temp_df = pd.DataFrame(results.fetchall(), columns=results.keys())

#         if len(temp_df) > 0:
#             new_df = pd.concat([temp_df, select_hdf], ignore_index=True)
#             hdf[table] = new_df
#             hdf.flush()

#         return hdf


def get_tournament(game_id, league_id, schedule):
    """
    Gets the tournament id from the tournaments dataframe

    Args:
        game_id (int): game id
        league_id (int): league id
        schedule (date, time): date and time of game
    Returns:
        tournament_id (int): tournament id from given arguments
    Raises:
        IndexError: if the game is not under a tournament
    """
    global tournaments
    league_bool = tournaments.league_id == league_id
    start_date_bool = tournaments.start_date <= schedule.to_pydatetime().date()
    end_date_bool = tournaments.end_date >= schedule.to_pydatetime().date()
    if league_id == 1:
        parent_id_bool = ~tournaments.parent_id.isnull()
    else:
        parent_id_bool = tournaments.parent_id.isnull()

    try:
        tournament_id = tournaments.loc[
            league_bool & start_date_bool &
            end_date_bool & parent_id_bool, 'id'].values[0]

    except IndexError:
        tournament_id = None

    # print(parent_id_bool)

    return tournament_id


def transform_df(df):
    """
    Adds updated_x, updated_y, distance, angle, and shot_class into dataframe

    Args:
        df (pd.DataFrame): dataframe to transform

    Returns:
        df (pd.DataFrame): dataframe with added columns
    """

    diff_list = []
    # df = df.reset_index(drop=True)
    for value in df.itertuples():
        # # print(key)
        try:
            if value.action_type == 'game' and value.action_subtype == 'start':
                diff = 0
            elif value.action_type == 'substitution' and \
                    value.action_subtype == 'out':
                diff = 0
            elif df.action_type[
                df.index[sum(df.index < value.Index)+1]] == 'substitution' and \
                    df.action_subtype[df.index[sum(df.index < value.Index)+1]] == 'out':
                    diff = df.secs_remaining[df.index[sum(df.index < value.Index)-1]]\
                        - df.secs_remaining[df.index[sum(df.index < value.Index)+1]]
            else:
                diff = df.secs_remaining[df.index[sum(df.index < value.Index)-1]]\
                        - df.secs_remaining[df.index[sum(df.index < value.Index)]]

        except IndexError:
                diff = df.secs_remaining[df.index[sum(df.index < value.Index)-1]]\
                        - df.secs_remaining[df.index[sum(df.index < value.Index)]]


        finally:
            diff = max(diff, 0)
        diff_list.append(diff)

    df.loc[:, 'time_elapsed'] = diff_list

    x = df.x
    y = df.y

    distance = np.sqrt((x - 75) ** 2 + (y - 15.75) ** 2)

    pre_angle = (75 - x) / (15.75 - y)
    angle = np.arctan(pre_angle) * [
        1.0 if value < 15.75 else -1.0 for value in y
    ]

    # y_trans = [1.0 if value < 15.75 else -1.0 for value in y]
    # angle = angle * y_trans

    update_bool = (df.action_type == '3pt') & (distance < 68)

    update_x = np.array([
        68 * np.sin(a) + 75 if b
        else x for a, b, x in zip(angle, update_bool, x)
    ])

    update_y = np.array([
        68 * np.cos(a) + 15.75 if b
        else y for a, b, y in zip(angle, update_bool, y)
    ])

    update_distance = np.sqrt((update_x - 75) ** 2 + (update_y - 15.75) ** 2)

    pre_update_angle = (75 - update_x) / (15.75 - update_y)

    update_angle = np.arctan(pre_update_angle) * [
        1.0 if value < 15.75 else -1.0 for value in update_y
    ]

    shot_class = []

    for a, d, y in zip(df.action_type.tolist(), update_distance, update_y):
        if 12 >= d >= 0:
            shot_class.append('RA')
        elif 45 >= d > 12:
            shot_class.append('4-15 ft')
        elif d > 100:
            shot_class.append('heave')
        elif 68 >= d and d > 45 and a == '2pt':
            shot_class.append('15-22 ft 2pt')
        elif a == '3pt' and y <= 30:
            shot_class.append('corner 3')
        elif a == '3pt' and y > 30 and d <= 100:
            shot_class.append('above break 3')
        else:
            shot_class.append('None')

    df.loc[:, 'update_x'] = update_x
    df.loc[:, 'update_y'] = update_y
    df.loc[:, 'distance'] = distance
    df.loc[:, 'angle'] = update_angle
    df.loc[:, 'shot_class'] = shot_class

    return df


def calculate_lineup(df, debug=False):
    """
    Creates a new column for lineup data on a pbp dataframe given game number

    Args:
        df (pd.DataFrame): game_events df

    Returns:
        A tuple of the transformed data frame and the errors

    Raises:
        ValueError: if player is not in lineup when sub out

        AssertionError: when sub in puts more than 5 players on court
    """
    copy_df = copy.deepcopy(df)
    # game_id = df.game_id.unique()[0]
    t1_lineup = []
    t2_lineup = []
    t1_id = 0
    t2_id = 0
    errors = []
    indices_to_remove = []
    duplicate_count = 0
    error_count = 0
    error_counts = {}

    for value in df.itertuples():
        error = None
        d_error = None
        # If Game Start, set parameters
        if value.action_type == 'game' and value.action_subtype == 'start':
            t1_id = df.iloc[value.Index + 1]['team_id']
            t2_id = df.iloc[value.Index + 1]['opp_team_id']
            t1_temp = []
            t2_temp = []
            t1_error_temp = []
            t2_error_temp = []

        if value.action_type == 'substitution':
            if value.action_subtype == 'in':
                if value.Index in indices_to_remove:
                    continue
                else:
                    if value.team_id == t1_id:
                        try:
                            t1_temp.append(int(value.player_id))
                            t1_temp.sort()
                            assert len(t1_temp) <= 5
                        except AssertionError:
                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > ' \
                                    f'Sub in  of {value.player_id} ' \
                                    f'puts team {t1_id} at ' \
                                    'more than 5 players on court.'

                            errors.append(error)
                            t1_error_temp.append(int(value.player_id))
                            t1_error_temp.sort()
                            error_count += 1

                            pass

                    else:
                        try:
                            t2_temp.append(int(value.player_id))
                            t2_temp.sort()
                            assert len(t2_temp) <= 5
                        except AssertionError:
                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > ' \
                                    f'Sub in  of {value.player_id} ' \
                                    f'puts team {t2_id} at ' \
                                    'more than 5 players on court.'

                            errors.append(error)
                            t2_error_temp.append(int(value.player_id))
                            t2_error_temp.sort()
                            error_count += 1

                            pass

            elif value.action_subtype == 'out':
                if value.team_id == t1_id:
                    try:
                        t1_temp.remove(value.player_id)
                        assert len(t1_temp) < 5

                    #player is not in the lineup
                    except ValueError:
                        try:
                            index_to_remove = df.index[
                                (df.game_id == value.game_id) &
                                (df.team_id == value.team_id) &
                                (df.player_id == value.player_id) &
                                (df.period == value.period) &
                                (df.secs_remaining == value.secs_remaining) &
                                (df.action_type == 'substitution') &
                                (df.action_subtype == 'in')][0]

                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > '\
                                    f'Sub out of {value.player_id} ' \
                                    f'from team {t1_id} not allowed. '\
                                    'player not on list. Error removed.'

                            d_error = f'{index_to_remove} (Q{value.period}:' \
                                      f'{value.secs_remaining}) > '\
                                      f'Sub in  of {value.player_id} ' \
                                      f'from team {t1_id} is a duplicate. '\
                                      'Error removed.'

                            indices_to_remove.append(index_to_remove)
                            indices_to_remove.append(value.Index)
                            copy_df.drop(index_to_remove, inplace=True)
                            copy_df.drop(value.Index, inplace=True)
                            errors.append(error)
                            errors.append(d_error)
                            duplicate_count += 1

                            continue

                        #error sub out has no duplicate sub in
                        except IndexError:
                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > ' \
                                    f'player {value.player_id}, ' \
                                    f'from {t1_id}, ' \
                                    'is not on list and has no duplicate. ' \
                                    'Error removed.'

                            copy_df.drop(value.Index, inplace=True)
                            errors.append(error)
                            error_count += 1

                            continue

                    except AssertionError:
                        error = f'{value.Index} (Q{value.period}:'\
                                f'{value.secs_remaining}) > '\
                                f'Sub out of {value.player_id} ' \
                                f'puts team {t1_id} at ' \
                                'more than 5 players on court.'

                        errors.append(error)
                        t1_error_temp.append(int(value.player_id))
                        t1_error_temp.sort()
                        error_count += 1

                else:
                    try:
                        t2_temp.remove(value.player_id)
                        assert len(t2_temp) < 5

                    #player is not in the lineup
                    except ValueError:
                        try:
                            index_to_remove = df.index[
                                (df.game_id == value.game_id) &
                                (df.team_id == value.team_id) &
                                (df.player_id == value.player_id) &
                                (df.period == value.period) &
                                (df.secs_remaining == value.secs_remaining) &
                                (df.action_type == 'substitution') &
                                (df.action_subtype == 'in')][0]

                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > '\
                                    f'Sub out of {value.player_id} ' \
                                    f'from team {t2_id} not allowed. '\
                                    'player not on list. Error removed.'

                            d_error = f'{index_to_remove} (Q{value.period}:' \
                                      f'{value.secs_remaining}) > '\
                                      f'Sub in  of {value.player_id} ' \
                                      f'from team {t2_id} is a duplicate. '\
                                      'Error removed.'

                            indices_to_remove.append(index_to_remove)
                            indices_to_remove.append(value.Index)
                            copy_df.drop(index_to_remove, inplace=True)
                            copy_df.drop(value.Index, inplace=True)
                            errors.append(error)
                            errors.append(d_error)
                            duplicate_count += 1

                            continue

                        #error sub out has no duplicate sub in
                        except IndexError:
                            error = f'{value.Index} (Q{value.period}:' \
                                    f'{value.secs_remaining}) > ' \
                                    f'player {value.player_id}, ' \
                                    f'from {t2_id}, ' \
                                    'is not on list and has no duplicate. ' \
                                    'Error removed.'

                            copy_df.drop(value.Index, inplace=True)
                            errors.append(error)
                            error_count += 1

                            continue

                    except AssertionError:
                        error = f'{value.Index} (Q{value.period}:'\
                                f'{value.secs_remaining}) > '\
                                f'Sub out of {value.player_id} ' \
                                f'puts team {t2_id} at ' \
                                'more than 5 players on court.'

                        errors.append(error)
                        t2_error_temp.append(int(value.player_id))
                        t2_error_temp.sort()
                        error_count += 1


        else:
            pass

        try:
            if len(t1_temp) < 5 or len(t2_temp) < 5:
                assert value.time_elapsed == 0
        except AssertionError:
            error = f'{value.Index} (Q{value.period}:{value.secs_remaining}) > '\
            'lineups with less than 4 players and have time elapsed.'
            errors.append(error)
            error_count += 1

        t1_lineup.append(t1_temp[:])
        t2_lineup.append(t2_temp[:])

        if debug:
            print(f'{value.Index} (Q{value.period}:{value.secs_remaining})'
                  f' > {t1_temp} <> {t2_temp} : {t1_error_temp} <>'
                  f' {t2_error_temp} > {error}')

    if len(copy_df) == len(t1_lineup):
        copy_df.loc[:, 't1_lineup'] = t1_lineup

    if len(copy_df) == len(t2_lineup):
        copy_df.loc[:, 't2_lineup'] = t2_lineup

    indices_to_remove.sort()

    error_counts['duplicates'] = duplicate_count
    error_counts['errors'] = error_count

    return copy_df, errors, error_counts


def clean_df(tournament_id, game_type=0, deletions=None):
    """
    clean dataframe of a specific tournament id and game type

    Args:
        tournament_id (int): tournament id
        game_type (int): game_type.
                         0 is elimination game, 1 is playoff game.
                         Defaults to 0

    Returns:
        df with new columns

    """
    games_list = games.loc[
        (games.tournament_id == tournament_id) &
        (games.game_type == 0)].id.tolist()

    df_list = [game_events.loc[
        game_events.game_id == game_id
    ].reset_index(drop=True) for game_id in games_list]

    to_delete = None

    if deletions is not None:
        try:
            to_delete = deletions[tournament_id]

        except (KeyError, TypeError):
            to_delete = None


    errors = {}
    error_counts = {}
    error_counts['duplicates'] = 0
    error_counts['errors'] = 0
    df_clean_list = []

    for df, game_id in zip(df_list, games_list):
        print(game_id)

        if to_delete is not None:
            try:
                df.drop(to_delete[game_id], inplace=True)
                print(to_delete[game_id])
            except KeyError:
                pass

        df = transform_df(df)
        copy_df, error, error_count = calculate_lineup(df)
        g_id = df.game_id.unique()[0]
        df_clean_list.append(copy_df)

        if len(error) > 0:
            errors[g_id] = error

        if error_count['duplicates'] > 0 or error_count['errors'] > 0:
            error_counts['duplicates'] += error_count['duplicates']
            error_counts['errors'] += error_count['errors']
    clean_df = pd.concat(df_clean_list)

    return clean_df, errors, error_counts


if not os.path.isfile('hb_db.h5'):
    with pd.HDFStore('hb_db.h5') as hdf:
        tables = [
            'leagues', 'players', 'game_player_stats',
            'game_team_stats', 'teams', 'tournaments', 'game_events',
            'games'
        ]

        for table in tables:
            # print(table)
            temp_table = download(table)

            hdf.put(key=table, value=temp_table)

leagues = pd.read_hdf('hb_db.h5', 'leagues')
players = pd.read_hdf('hb_db.h5', 'players')
game_player_stats = pd.read_hdf('hb_db.h5', 'game_player_stats')
game_team_stats = pd.read_hdf('hb_db.h5', 'game_team_stats')
teams = pd.read_hdf('hb_db.h5', 'teams')
tournaments = pd.read_hdf('hb_db.h5', 'tournaments')
game_events = pd.read_hdf('hb_db.h5', 'game_events')
games = pd.read_hdf('hb_db.h5', 'games')

print('File initialized')

games['tournament_id'] = games.apply(
    lambda row: get_tournament(
        row['id'],
        row['league_id'],
        row['schedule']
    ), axis=1
)

pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)

#Deletions
deletions = {}
deletions[26] = {1424:[166, 236, 261, 262, 263, 265, 266, 291, 293, 294, 319]}
