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
-include_lib("emysql/include/emysql.hrl").

%% API
-export([
  start_link/2
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

start_link(MQPid, DBRef) ->
  gen_server:start_link(?MODULE, [MQPid, DBRef], []).


%%%====================================
%%% GenServer Callback
%%%====================================
init([MQPid, DBRef]) ->
  game_tiny_mq:listen(MQPid),
  {ok, {MQPid, DBRef}}.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({tiny_mq_message, Msg}, {MQPid, DBRef} = State)
  when is_tuple(Msg)->
  insert_to_db(DBRef, Msg),
  game_tiny_mq:ack(MQPid),
  {noreply, State};
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
  case fetch_prepare(Name) of
    undefined ->
      prepare_sql(Name, length(Values));
    _ ->
      ok
  end,
  execute_ok(DBRef, Name, Values).


prepare_sql(Name, ValueCount) ->
  Values = string:join(["?" || _ <- lists:seq(1, ValueCount)], ","),
  PrepareSql = list_to_binary("insert into " ++ atom_to_list(Name) ++ "  value (" ++ Values ++ ");"),
  emysql:prepare(Name, PrepareSql),
  Name.

fetch_prepare(Name) ->
  emysql_statements:fetch(Name).


  -spec execute_ok(atom()|pid(), atom(), [any()])->ok | {error, any()}.
execute_ok(DBRef, StmtName, Args)->
  Ok = emysql:execute(DBRef, StmtName, Args),
  case Ok of
    #ok_packet{}->ok;
    _->
      error(Ok)
  end.
