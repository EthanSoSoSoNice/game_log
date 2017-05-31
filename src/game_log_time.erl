-module(game_log_time).

-export([
	timestamp/0,
	millisecond/0,
	state_time/0,
	local_form_utc/1,
	local_zero_time/1,
	utc_fixed_time/2,
	string_to_datetime/1,
	string_to_time/1,
	string_to_date/1,
	convert_today_timestamp/1,
	timestamp_convert_date/1,
	datetime/0
]).

-record(state, {
	utc_1970_secs,
	loc_1970_secs,
	local_delta
}).

-spec state_time() -> #state{}.
state_time() ->
	UTC1970 = {{1970,1,1}, {0,0,0}},
	LOC1970 = calendar:universal_time_to_local_time(UTC1970),
	UTC1970Secs = calendar:datetime_to_gregorian_seconds(UTC1970),
	LOC1970Secs = calendar:datetime_to_gregorian_seconds(LOC1970),
	DeltaSec = LOC1970Secs - UTC1970Secs,
	#state{
		utc_1970_secs = UTC1970Secs,
		loc_1970_secs = LOC1970Secs,
		local_delta = DeltaSec
	}.

-spec timestamp()->non_neg_integer().
timestamp()->
	{M, S, _} = os:timestamp(),
	M * 1000000 + S.

millisecond() ->
	{M, S, SS} = os:timestamp(),
	trunc(M * 1000000000 + S * 1000 + SS / 1000).


local_form_utc(TS)->
  State = state_time(),
  TS - State#state.loc_1970_secs - State#state.local_delta.

utc_fixed_time(Date,RefTime) ->
	State = state_time(),
	calendar:datetime_to_gregorian_seconds({Date, RefTime}) - State#state.utc_1970_secs - State#state.local_delta.

local_zero_time(TS)->
  local_zero_time(state_time(),TS).

local_zero_time(State,TS) ->
	TS + State#state.local_delta + State#state.utc_1970_secs.



%% string to datetime
%% exmple "1997-1-25 10:10:10"
string_to_datetime(DateTimeStr) ->
	[DateStr, TimeStr] = string:tokens(DateTimeStr, " "),
  [Y, M, D] = string:tokens(DateStr, "-"),
	Date = {list_to_integer(Y), list_to_integer(M), list_to_integer(D)},
	Time = string_to_time(TimeStr),
	{Date, Time}.

string_to_time(TimeStr) ->
	case string:tokens(TimeStr, ":") of
		[H, MM] ->
			{list_to_integer(H), list_to_integer(MM), 0};
		[H, MM, SS] ->
			{list_to_integer(H), list_to_integer(MM), list_to_integer(SS)}
	end.

string_to_date(DateStr) ->
	[Y, M, D] = string:tokens(DateStr, "-"),
	{list_to_integer(Y), list_to_integer(M), list_to_integer(D)}.

convert_today_timestamp({H, M, S} = Time) when is_integer(H) andalso is_integer(M) andalso is_integer(S) ->
	State = state_time(),
	TS = timestamp(),
	CurTS = local_zero_time(State,TS),
	{Date,_} = calendar:gregorian_seconds_to_datetime(CurTS),
	utc_fixed_time(Date,Time).

timestamp_convert_date(Timestamp)->
	LocalTime = local_zero_time(Timestamp),
	{Date, _} = calendar:gregorian_seconds_to_datetime(LocalTime),
	Date.

datetime()->
  {Date, Time} = calendar:local_time(),
  {YY, MM, DD} = Date,
  {HH, Mm, SS} = Time,
  <<(integer_to_binary(YY))/binary, $-, (integer_to_binary(MM))/binary, $-, (integer_to_binary(DD))/binary, 16#20,
    (integer_to_binary(HH))/binary, $:, (integer_to_binary(Mm))/binary, $:, (integer_to_binary(SS))/binary>>.
