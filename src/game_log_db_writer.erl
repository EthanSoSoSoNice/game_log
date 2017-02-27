%%%-------------------------------------------------------------------
%%% @author 11726
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. 二月 2017 10:05
%%%-------------------------------------------------------------------
-module(game_log_db_writer).
-author("11726").
-behaviour(gen_server).
-include("game_log.hrl").

%% API
-export([
  start_link/1
]).

%% gen_server callback
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  code_change/3,
  terminate/2
]).

%%%====================================
%%% API
%%%====================================

start_link(MQPid) ->
  gen_server:start_link(?MODULE, [MQPid], []).


%%%====================================
%%% GenServer Callback
%%%====================================
init([MQPid]) ->
  game_tiny_mq:listen(MQPid),
  {ok, MQPid}.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({tiny_mq_message, Msg}, MQPid)
  when is_tuple(Msg)->
  insert_to_db(game_log_database:get_pool_ref(), Msg),
  game_tiny_mq:ack(MQPid),
  {noreply, MQPid};
handle_info({tiny_mq_message, _Msg}, {MQPid, _} = State) ->
  game_tiny_mq:ack(MQPid),
  {noreply, State};
handle_info(_Msg, State) ->
  {noreply, State}.

code_change(_Old, State, _Extra) ->
  {ok, State}.

terminate(_, _) ->
  ok.

insert_to_db(DBRef, Record) ->
  [Name|Values] = tuple_to_list(Record),
  case game_log_db_mgr:fetch_prepare(Name) of
    undefined ->
      game_log_db_mgr:prepare_sql(Name, length(Values));
    _ ->
      ok
  end,
  game_log_db_mgr:execute_ok(DBRef, Name, Values).
