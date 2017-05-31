%%%-------------------------------------------------------------------
%%% @author WangWeiNing
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 五月 2017 13:49
%%%-------------------------------------------------------------------
-module(game_log_config).
-author("11726").

%% API
-export([
  init/0,
  get_config/1,
  get_config/2,
  set_config/2
]).

-type config_key() :: term().
-type config_value() :: term().
-type config_default_value() :: term().

init() ->
  ets:new(?MODULE, [named_table, public, {keypos, 1}]),
  ok.

-spec get_config(config_key()) -> undefined | config_value().
get_config(K) ->
  case ets:lookup(?MODULE, K) of
    [] ->
      undefined;
    [{_, V}] ->
      V
  end.

-spec get_config(config_key(), config_default_value()) -> config_value() | config_default_value().
get_config(K, Default) ->
  case get_config(K) of
    undefined ->
      Default;
    V ->
      V
  end.

-spec set_config(config_key(), config_value()) ->ok.
set_config(K, V) ->
  ets:insert(?MODULE, {K, V}).
