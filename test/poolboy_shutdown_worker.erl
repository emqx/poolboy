-module(poolboy_shutdown_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    {ok, proplists:get_value(test_parent, Args)}.

handle_call(_Request, _From, Parent) ->
    {reply, ok, Parent}.

handle_cast(_Request, Parent) ->
    {noreply, Parent}.

handle_info(_Message, Parent) ->
    {noreply, Parent}.

terminate(_Reason, Parent) ->
    Parent ! {shutdown_started, self()},
    receive finish_shutdown -> ok after 2000 -> ok end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
