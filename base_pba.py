from sqlalchemy import *
import pandas as pd
import copy
import os.path


def connect_hb():
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


def calculate_lineup(df):
    """
    Creates a new column for lineup data on a pbp dataframe given game number

    Args:
        game_id: the game id of the df to be transformed

    Returns:
        A tuple of the transformed data frame and the errors

    Raises:
        Value Error: if player is not in lineup when sub out

        Assertion Error: when sub in puts more than 5 players on court
    """
    copy_df = copy.deepcopy(df)
    t1_lineup = []
    t2_lineup = []
    t1_id = 0
    t2_id = 0
    errors = []
    keys_to_remove = []

    for key, value in enumerate(df.itertuples()):
        error = None

        if value.action_type == 'game' and value.action_subtype == 'start':
            # print(f'{key}: game start!')
            t1_id = df.iloc[key + 1, 2]
            t2_id = df.iloc[key + 1, 3]
            # message = f'{key}: Game Start'
            t1_temp = []
            t2_temp = []

        # print(f'{message} => {t1_temp} : {t2_temp}')

        if value.action_type == 'substitution':
            if value.action_subtype == 'in':
                if key in keys_to_remove:
                    continue
                else:
                    if value.team_id == t1_id:
                        try:
                            # message = f'{key}: sub in by team {t1_id}'
                            t1_temp.append(int(value.player_id))
                            t1_temp.sort()
                            assert len(t1_temp) <= 5
                        except AssertionError:
                            error = f'{key}: Sub in of {value.player_id} ' \
                                    f'puts team {t1_id} at ' \
                                    'more than 5 players on court.'
                            # print(error)
                            errors.append(error)

                            continue

                    else:
                        try:
                            # message = f'{key}: sub in by team {t2_id}'
                            t2_temp.append(int(value.player_id))
                            t2_temp.sort()
                            assert len(t2_temp) <= 5
                        except AssertionError:
                            error = f'{key}: Sub in of {value.player_id} ' \
                                    f'puts team {t2_id} at ' \
                                    'more than 5 players on court.'
                            # print(error)
                            errors.append(error)

                            continue

            elif value.action_subtype == 'out':
                if value.team_id == t1_id:
                    try:
                        # message = f'{key}: sub out by team {t1_id}'
                        t1_temp.remove(value.player_id)
                    except ValueError:
                        key_to_remove = df.index[
                            (df.game_id == value.game_id) &
                            (df.team_id == value.team_id) &
                            (df.player_id == value.player_id) &
                            (df.period == value.period) &
                            (df.secs_remaining == value.secs_remaining) &
                            (df.action_type == 'substitution') &
                            (df.action_subtype == 'in')][0]

                        error = f'{key} & {key_to_remove}: ' \
                                f'player {value.player_id}, ' \
                                f'from {t1_id}, ' \
                                'is not on list.'

                        keys_to_remove.append(key_to_remove)
                        keys_to_remove.append(key)

                        errors.append(error)

                        copy_df.drop(key_to_remove, inplace=True)

                        copy_df.drop(df.index[key], inplace=True)
                        continue

                else:
                    try:
                        # message = f'{key}: sub out by team {t2_id}'
                        t2_temp.remove(value.player_id)
                    except ValueError:
                        key_to_remove = df.index[
                            (df.game_id == value.game_id) &
                            (df.team_id == value.team_id) &
                            (df.player_id == value.player_id) &
                            (df.period == value.period) &
                            (df.secs_remaining == value.secs_remaining) &
                            (df.action_type == 'substitution') &
                            (df.action_subtype == 'in')][0]

                        error = f'{key} & {key_to_remove}: ' \
                                f'player {value.player_id}, ' \
                                f'from {t2_id}, ' \
                                'is not on list.'

                        keys_to_remove.append(key_to_remove)
                        keys_to_remove.append(key)

                        errors.append(error)

                        copy_df.drop(key_to_remove, inplace=True)

                        copy_df.drop(df.index[key], inplace=True)
                        continue

        else:
            pass
            # message = f'{key}: No sub'
            # print(messages)

        # print(f'{key} = {value}: {t1_temp} = {t2_temp}')
        # print(f'{hex_1} : {hex_2}')
        t1_lineup.append(t1_temp[:])
        t2_lineup.append(t2_temp[:])

    if len(copy_df) == len(t1_lineup):
        copy_df.loc[:, 't1_lineup'] = t1_lineup

    if len(copy_df) == len(t2_lineup):
        copy_df.loc[:, 't2_lineup'] = t2_lineup

    keys_to_remove.sort()
    # print(keys_to_remove)
    print(len(copy_df), len(t1_lineup), len(t2_lineup))
    print(errors)
    # return copy_df, t1_lineup, t2_lineup, errors
    return copy_df, errors


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

print('HDF File initialized')

games['tournament_id'] = games.apply(
    lambda row: get_tournament(
        row['id'],
        row['league_id'],
        row['schedule']
    ), axis=1
)

pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
