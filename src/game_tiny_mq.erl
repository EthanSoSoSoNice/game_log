%%%-------------------------------------------------------------------
%%% @author 11726
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%% one process corresponding to a queue
%%% @end
%%% Created : 15. 二月 2017 17:45
%%%-------------------------------------------------------------------
-module(game_tiny_mq).
-author("11726").

%% API
-export([
  start/0,
  start/1,
  start_link/0,
  start_link/1,
  listen/1,
  send_message/2,
  ack/1
]).

%% internal export
-export([init/1]).

-record(consumer, {
  pid,
  mref
}).

-record(message, {
  id,
  msg
}).

-record(state, {
  free = [],
  busy = [],
  consumers = [] :: [#consumer{}],
  msg_queue = queue:new(),
  msg_count = 0 :: integer()
}).


%%%==========================================
%%% API
%%%==========================================

start() ->
  proc_lib:start(?MODULE, init, [undefined]).

start(Name) ->
  case where(Name) of
    undefined ->
      proc_lib:start(?MODULE, init, [Name]);
    Pid ->
      {error, {already_started, Pid}}
  end.

start_link() ->
  proc_lib:start_link(?MODULE, init, [undefined]).

start_link(Name) ->
  case where(Name) of
    undefined ->
      proc_lib:start_link(?MODULE, init, [Name]);
    Pid ->
      {error, {already_started, Pid}}
  end.

listen(Pid)
  when is_pid(Pid) ->
  send_msg(Pid, {listen, self()});
listen(Name)
  when is_atom(Name) ->
  case whereis(Name) of
    undefined ->
      exit(noproc);
    Pid ->
      listen(Pid)
  end.


send_message(Pid, Msg)
  when is_pid(Pid) ->
  send_msg(Pid, {message, Msg});
send_message(Name, Msg)
  when is_atom(Name) ->
  case whereis(Name) of
    undefined ->
      exit(noproc);
    Pid ->
      send_message(Pid, Msg)
  end.

ack(Pid)
  when is_pid(Pid) ->
  send_msg(Pid, {ack, self()});
ack(Name)
  when is_atom(Name) ->
  case whereis(Name) of
    undefined ->
      exit(noproc);
    Pid ->
      ack(Pid)
  end.

%%%==========================================
%%% Internal Function
%%%==========================================

init(Name) ->
  case Name of
    undefined ->
      proc_lib:init_ack({ok, self()}),
      loop(#state{});
    _ ->
      case name_register(Name) of
        true ->
          proc_lib:init_ack({ok, self()}),
          loop(#state{});
        {false, Pid} ->
          proc_lib:init_ack({error, {already_started, Pid}})
      end
  end.

loop(State) ->
  receive
    {'mq_proto', Msg} ->
      case catch handle_msg(Msg, State) of
        {'EXIT', Error} ->
          error_logger:error_msg("Error ~p~nMessage~p~nStccktrace", [Error, Msg, erlang:get_stacktrace()]),
          exit(error);
        State1 ->
          loop(State1)
      end;
    {'DOWN', _Ref, process, Pid, _} ->
      loop(handle_down(Pid, State))
  end.


handle_msg({ack, Consumer}, #state{ busy = BusyList, free = FreeList } = State) ->
  case lists:keyfind(Consumer, 1, BusyList) of
    false ->
      State;
    {_, _} ->
      FreeList1 = [Consumer|FreeList],
      BusyList1 = lists:keydelete(Consumer, 1, BusyList),
      push_message(State#state{ free = FreeList1, busy = BusyList1 })
  end;
handle_msg({message, Msg}, #state{ free = [], msg_queue = MsgQueue, msg_count = MsgCount } = State) ->
  NewCount = MsgCount + 1,
  Message = #message{ id = generate_id(NewCount), msg = Msg },
  State#state{
    msg_queue = queue:in(Message, MsgQueue),
    msg_count = NewCount
  };
handle_msg({message, Msg}, #state{ free = _, msg_queue = MsgQueue, msg_count = MsgCount } = State) ->
  NewCount = MsgCount + 1,
  Message = #message{ id = generate_id(NewCount), msg = Msg },
  State1 = State#state{
    msg_queue = queue:in(Message, MsgQueue),
    msg_count = NewCount
  },
  push_message(State1);
handle_msg({listen, Consumer}, State) ->
  #state{
    consumers = ConsumerList,
    msg_queue = MsgQueue
  } = State,
  case lists:keyfind(Consumer, #consumer.pid, ConsumerList) of
    false ->
      State1 = do_listen(Consumer, State),
      case queue:len(MsgQueue) > 0 of
        true ->
          push_message(State1);
        false ->
          State1
      end;
    _ ->
      State
  end.

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
  ConsumerList1 = [#consumer{ pid = Consumer, mref = MRef}|ConsumerList],
  State#state{ consumers = ConsumerList1, free = [Consumer|FreeList] }.

do_push_message(Consumer, Message, State) ->
  Consumer ! {tiny_mq_message, {Message#message.id, Message#message.msg}},
  Busy = State#state.busy,
  Busy1 = [{Consumer, Message}|Busy],
  State#state{ busy = Busy1 }.


generate_id(Sequence) ->
  <<
    (timestamp()):32,
    (integer_to_binary(Sequence)):32
  >>.


send_msg(Pid, Msg) ->
  Pid ! {'mq_proto', Msg}.


name_register({local, Name} = LN) ->
  try register(Name, self()) of
    true -> true
    catch
    error: _ ->
      {false, where(LN)}
  end;
name_register({global, Name} = GN) ->
  case global:register_name(Name, self()) of
    yes -> true;
    no -> {false, where(GN)}
  end;
name_register({via, Module, Name} = GN) ->
  case Module:register_name(Name, self()) of
    yes ->
      true;
    no ->
      {false, where(GN)}
  end.


where({global, Name}) -> global:whereis_name(Name);
where({vai, Module, Name}) -> Module:whereis_name(Name);
where({local, Name}) -> whereis(Name).
