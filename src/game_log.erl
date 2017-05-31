%%%-------------------------------------------------------------------
%%% @author WangWeiNing
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. 五月 2017 13:56
%%%-------------------------------------------------------------------
-module(game_log).
-author("11726").

%% API
-export([
  start/3,
  start_link/3,
  send_message/2
]).


-type game_log_option() :: {protection, integer()} | {protection_window, integer()}.
%%===========================================
%% API
%%===========================================

-spec start(atom(), [pid()], [game_log_option()]) -> {ok, atom()}.
start(Name, WorkerList, Options) ->
  start(Name, WorkerList, Options, false).

start(Name, WorkerList, Options, Link) ->
  Protection = proplists:get_value(protection, Options),
  PWindow = proplists:get_value(protection_window, Options),
  if
    Link -> {ok, _} = game_log_mq:start_link(Name, [{protection, Protection}, {protection_window, PWindow}]);
    true -> {ok, _} = game_log_mq:start(Name, [{protection, Protection}, {protection_window, PWindow}])
  end,
  lists:foreach(
    fun(Handler) ->
      ok = game_log_mq:listen(Name, Handler)
    end, WorkerList),
  {ok, Name}.

-spec start_link(atom(), [pid()], [game_log_option()]) -> {ok, atom()}.
start_link(Name, WorkerList, Options) ->
  start(Name, WorkerList, Options, true).

-spec send_message(atom(), term()) -> ok.
send_message(Ref, Message) ->
  Pid = whereis(Ref),
  Async = game_log_config:get_config({Pid, async}, true),
  case Async of
    true ->
      game_log_mq:send_message(Ref, Message);
    _ ->
      game_log_mq:sync_send_message(Ref, Message)
  end.




%%===========================================
%% Internal
%%===========================================
