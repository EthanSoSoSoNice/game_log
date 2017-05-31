%%%-------------------------------------------------------------------
%%% @author WangWeiNing
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. 二月 2017 13:41
%%%-------------------------------------------------------------------
-module(game_db_writer_sup).
-author("11726").
-behaviour(supervisor).

%% API
-export([
  start_child/1,
  start_link/0,
  stop/1
]).

%% internal export
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
  ok.

init([]) ->
  Children = {game_log_db_writer, {game_log_db_writer, start_link, []}, permanent, 5000, worker, [game_log_db_writer]},
  {ok, {{simple_one_for_one, 5, 5}, [Children]}}.


start_child(MQPid) ->
  supervisor:start_child(?MODULE, [MQPid]).

