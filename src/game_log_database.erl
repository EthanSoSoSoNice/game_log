%%%-------------------------------------------------------------------
%%% @author WangWeiNing
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 二月 2017 11:13
%%%-------------------------------------------------------------------
-module(game_log_database).
-author("11726").

-compile(export_all).
%% API
-export([start_link/2]).
-type pool_ref() :: atom().
-type database_option() ::
{database_user, list()} |
{database_pwd,  list()} |
{database_poo_size, integer()} |
{database_addr, list()} |
{database_port, integer()} |
{database_name, list()} |
{database_encode, utf8 | latin1 | {utf8, utf8_unicode_ci} | {utf8, utf8_general_ci}}.

%% gen_server callback
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  code_change/3,
  terminate/2
]).

-define(DB_CREATE(Name), <<"CREATE DATABASE IF NOT EXISTS ", (list_to_binary(Name))/binary, " DEFAULT CHARACTER SET UTF8 COLLATE UTF8_GENERAL_CI;">>).

-record(state, {
  ref :: pool_ref(),
  old_ref :: pool_ref()
}).



%%%===================================
%%% API
%%%===================================
%% @doc
%% Options :: [database_option()]
%% @end
start_link(SqlFile, Options)->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [SqlFile, Options], []).

-spec set_pool_ref(pool_ref()) -> ok.
set_pool_ref(Ref) ->
  ets:insert(?MODULE, {pool_ref, Ref}).

-spec get_pool_ref() -> undefined | pool_ref().
get_pool_ref() ->
  get_option(pool_ref, undefined).

%%%===================================
%%% gen_server callback
%%%===================================

init([SqlFile, Options]) ->
  ets:new(?MODULE, [named_table, public]),
  DBName = proplists:get_value(database_name, Options),
  POOL 	= proplists:get_value(database_pool_size, Options, 8),
  UN 		= proplists:get_value(database_user, Options),
  PW 		= proplists:get_value(database_pwd, Options),
  ADDR 	= proplists:get_value(database_addr, Options),
  PORT 	= proplists:get_value(database_port, Options, 3306),
  Encode = proplists:get_value(database_encode, Options, utf8),
  ets:insert(?MODULE, [
    {db_name, DBName},
    {db_pool_size, POOL},
    {db_account, UN},
    {db_password, PW},
    {db_addr, ADDR},
    {db_port, PORT},
    {db_sql_file, SqlFile},
    {db_encode, Encode}
  ]),
  Ref = start_db(),
  set_pool_ref(Ref),
  start_refresh_timer(),
  {ok, #state{ ref = Ref }}.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({timeout, _, refresh}, State) ->
  OldRef = State#state.ref,
  NewRef = start_db(),
  set_pool_ref(NewRef),
  start_clear_timer(),
  start_refresh_timer(),
  {noreply, State#state{ old_ref = OldRef, ref = NewRef }};
handle_info({timeout, _, clear_timeout}, State) ->
  case State#state.old_ref of
    undefined ->
      ok;
    _ ->
      clear_pool(State#state.old_ref)
  end,
  {noreply, State};
handle_info(_Msg, State) ->
  {noreply, State}.

code_change(_Old, State, _Extra) ->
  {ok, State}.

terminate(_, _) ->
  ok.

%%%===================================
%%% Internal Function
%%%===================================

get_option(Name) ->
  get_option(Name, undefined).

get_option(Name, Default) ->
  case ets:lookup(?MODULE, Name) of
    [] ->
      Default;
    [{_, V}] ->
      V
  end.

start_db() ->
  DBName = database_name(get_option(db_name)),
  SqlFile = get_option(db_sql_file),
  Ref = list_to_atom(DBName),
  PoolSize = get_option(db_pool_size, 8),
  User = get_option(db_account),
  Pwd = get_option(db_password),
  Addr = get_option(db_addr),
  Port = get_option(db_port, 3306),
  Sql = create_db_sql(DBName, SqlFile),
  Cmds = binary:split(Sql, [<<";">>], [global]),
  Encode = get_option(db_encode),
  emysql:add_pool(Ref, PoolSize, User, Pwd, Addr, Port, undefined, Encode, [Cmd || Cmd <- Cmds, Cmd =/= <<>>]),
  Ref.

database_name(Name) ->
  {{Y, M, D}, _} = calendar:local_time(),
  Name ++ to_list(Y) ++ to_list(M) ++ to_list(D).

to_list(N) when N < 10 ->
  [$0, $0 + N];
to_list(N) ->
  integer_to_list(N).

create_db_sql(DBName, SqlFile) ->
  {ok, SqlContent} = file:read_file(SqlFile),
  <<
    (?DB_CREATE(DBName))/binary,
    "\n",
    "use ", (list_to_binary(DBName))/binary, ";",
    "\n",
    (SqlContent)/binary
  >>.

clear_pool(PoolRef) ->
  emysql:remove_pool(PoolRef).

start_refresh_timer() ->
  CurrentTS = game_log_time:timestamp(),
  RefreshTS = game_log_time:convert_today_timestamp({0, 0, 0}) + (24 * 60 * 60),
  erlang:start_timer((RefreshTS - CurrentTS) * 1000, self(), refresh).

start_clear_timer() ->
  erlang:start_timer((60 * 1000 * 10), self(), clear_timeout).

