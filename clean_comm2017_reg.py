from base_pba import *


comm2017 = games.loc[
    (games.tournament_id == 27) & (games.game_type == 0)].id.tolist()

# for game_id in comm2017:
#     print(game_id)

comm2017_df_list = [
    game_events.loc[
        game_events.game_id == game_id
    ].reset_index(drop=True) for game_id in comm2017
]

# errors_df_list = [
#     f'{game_id}_errors' for game_id in comm2017
# ]

# comm2017_df_clean_list = [
#     f'{game_id}_clean' for game_id in comm2017
# ]

errors = {}
comm2017_df_clean_list = []
for df in comm2017_df_list:
    copy_df, error = calculate_lineup(df)
    copy_df = transform_df(copy_df)
    g_id = df.game_id.unique()[0]
    comm2017_df_clean_list.append(copy_df)
    if len(error) > 0:
        errors[g_id] = error

clean_df = pd.concat(comm2017_df_clean_list)

del copy_df, error, comm2017_df_clean_list
