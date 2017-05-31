%%%-------------------------------------------------------------------
%%% @author WangWeiNing
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. 二月 2017 17:45
%%%-------------------------------------------------------------------
-module(game_log_mq).
-author("11726").
-behaviour(gen_server).

%% API
-export([
  start/1,
  start/2,
  start_link/1,
  start_link/2,
  listen/2,
  send_message/2,
  sync_send_message/2,
  ack/1
]).

%% internal export
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
  ]).

-record(consumer, {
  pid,
  mref
}).

-record(message, {
  id,
  msg,
  sender = undefined
}).

-record(state, {
  free = [],
  busy = [],
  consumers = [] :: [#consumer{}],
  msg_queue = queue:new(),
  msg_count = 0 :: integer(),
  msg_id = 0 :: integer(),
  protection = 0 :: integer(),
  protection_window = 0 :: integer()
}).


%%%==========================================
%%% API
%%%==========================================

start(Args) ->
  gen_server:start(?MODULE, Args, []).

start(Name, Args) ->
  gen_server:start({local, Name}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

start_link(Name, Args) ->
  gen_server:start_link({local, Name}, ?MODULE, Args, []).

listen(Ref, ConsumerRef) ->
  gen_server:call(Ref, {listen, ConsumerRef}).

send_message(Ref, Msg) ->
  gen_server:cast(Ref, {message, Msg}).

sync_send_message(Ref, Msg) ->
  gen_server:call(Ref, {message, Msg}).

ack(Ref) ->
  gen_server:cast(Ref, {ack, self()}).


%%%==========================================
%%% Callback
%%%==========================================
init(Args) ->
  Protection = proplists:get_value(protection, Args, 0),
  PWindow = proplists:get_value(protection_window, Args, 0),
  {ok, #state{
    protection = Protection,
    protection_window = PWindow
  }}.

handle_call({listen, Consumer}, _From, State) ->
  #state{
    consumers = ConsumerList,
    msg_queue = MsgQueue
  } = State,
  case lists:keyfind(Consumer, #consumer.pid, ConsumerList) of
    false ->
      State1 = do_listen(Consumer, State),
      case queue:len(MsgQueue) > 0 of
        true ->
          {reply, ok, push_message(State1)};
        false ->
          {reply, ok, State1}
      end;
    _ ->
      {reply, ok, State}
  end;
handle_call({message, Msg}, Sender, State) ->
  {noreply, do_message(Msg, Sender, State)};
handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast({ack, Consumer}, #state{ busy = BusyList, free = FreeList, msg_count = MsgCount } = State) ->
  #state{
    busy = BusyList,
    free = FreeList,
    msg_count = MsgCount,
    protection = Protection,
    protection_window = PWindow
  } = State,
  case lists:keyfind(Consumer, 1, BusyList) of
    false ->
      {noreply, State};
    {_, Message} ->
      FreeList1 = [Consumer|FreeList],
      BusyList1 = lists:keydelete(Consumer, 1, BusyList),
      case Message of
        #message{ sender = undefined } ->
          ok;
        #message{ sender = Sender } ->
          gen_server:reply(Sender, ok)
      end,
      NewMsgCount = MsgCount - 1,
      %% check overload protection
      Async = game_log_config:get_config({self(), async}, true),
      case {(Protection - PWindow) =< MsgCount, Async} of
        {true, false} ->
          game_log_config:set_config({self(), async}, true);
        _ ->
          ok
      end,
      {noreply, push_message(State#state{ free = FreeList1, busy = BusyList1, msg_count = NewMsgCount })}
  end;
handle_cast({message, Msg}, State) ->
  {noreply, do_message(Msg, State)}.


handle_info({'DOWN', _Ref, process, Pid, _}, State) ->
  {noreply, handle_down(Pid, State)}.

terminate(_, _) ->
  ok.

code_change(_Old, State, _Extra) ->
  {ok, State}.

%%%==========================================
%%% Internal Function
%%%==========================================

do_message(Content, Sender, State) ->
  #state{
    msg_queue = MsgQueue,
    msg_count = MsgCount,
    msg_id = MsgId,
    free = Free,
    protection = Protection
  } = State,
  NewMsgId = MsgId + 1,
  NewMsgCount = MsgCount + 1,
  Message = #message{ id = NewMsgId, msg = Content, sender = Sender },
  NewState = State#state{
    msg_queue = queue:in(Message, MsgQueue),
    msg_count = NewMsgCount,
    msg_id    = NewMsgId
  },
  %% check overload protection
  Async = game_log_config:get_config({self(), async}, true),
  case {MsgCount >= Protection, Async} of
    {true, true} ->
      game_log_config:set_config({self(), async}, false);
    _ ->
      ok
  end,
  case Free of
    [] ->
      NewState;
    _ ->
      push_message(NewState)
  end.

do_message(Content, State) ->
  do_message(Content, undefined, State).



handle_down(Pid, State) ->
  #state{
    consumers = Consumers,
    busy = Busy,
    msg_queue = MsgQueue
  } = State,
  case lists:keyfind(Pid, #consumer.pid, Consumers) of
    false ->
      State;
    #consumer{ } ->
      case lists:keyfind(Pid, 1, Busy) of
        {_, Message} ->
          State#state{ consumers = lists:keydelete(Pid, #consumer.pid, Consumers), msg_queue = queue:in(Message, MsgQueue), busy = lists:keydelete(Pid, 1, Busy) };
        _ ->
          State#state{ consumers = lists:keydelete(Pid, #consumer.pid, Consumers )}
      end
  end.


push_message(#state{ free = [] } = State) ->
  State;
push_message(#state{ msg_queue = MsgQueue, free = [FreeConsumer|T] } = State) ->
  case queue:out(MsgQueue) of
    {{value, Msg}, MsgQueue1} ->
      State1 = State#state{ free = T, msg_queue = MsgQueue1 },
      do_push_message(FreeConsumer, Msg, State1);
    {empty, _} ->
      State
  end.


do_listen(Consumer, State) ->
  MRef = erlang:monitor(process, Consumer),
  ConsumerList = State#state.consumers,
  FreeList = State#state.free,
  ConsumerList1 = [#consumer{ pid = Consumer, mref = MRef }|ConsumerList],
  State#state{ consumers = ConsumerList1, free = [Consumer|FreeList] }.

do_push_message(Consumer, Message, State) ->
  Consumer ! {game_log_message, {Message#message.id, Message#message.msg}},
  Busy = State#state.busy,
  Busy1 = [{Consumer, Message}|Busy],
  State#state{ busy = Busy1 }.


generate_id(Sequence) ->
  <<Id:64>> = <<
    (game_log_time:timestamp()):32,
    (Sequence):32
  >>,
  Id.

