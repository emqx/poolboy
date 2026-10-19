-module(poolboy_overflow_tests).
-include_lib("eunit/include/eunit.hrl").

retention_test_() ->
    [{atom_to_list(Strategy), fun() -> retention(Strategy) end}
     || Strategy <- [fifo, lifo]].

retention(Strategy) ->
    with_pool([{overflow_ttl, 150}, {overflow_check_period, 20}, {strategy, Strategy}],
        fun(Pool) ->
            A = poolboy:checkout(Pool),
            B = poolboy:checkout(Pool),
            ?assertEqual(full, poolboy:checkout(Pool, false)),
            checkin(Pool, B),
            ?assertEqual({ready, 1, 1, 1}, poolboy:status(Pool)),
            ?assert(is_process_alive(B)),
            ?assertEqual(B, poolboy:checkout(Pool)),
            checkin(Pool, B),
            await(fun() -> not is_process_alive(B) end),
            ?assert(is_process_alive(A)),
            ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool)),
            checkin(Pool, A)
        end).

immediate_reaping_test_() ->
    [{Name, fun() -> with_pool(Opts, fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        await(fun() -> not is_process_alive(B) end),
        ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool)),
        checkin(Pool, A)
    end) end} || {Name, Opts} <- [{"default", []}, {"zero", [{overflow_ttl, 0}]}]].

%% An unexpired, recently returned LIFO worker must not hide older idle workers.
lifo_reaping_test() ->
    with_pool([{overflow_ttl, 80}, {overflow_check_period, 1000}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, A),
        timer:sleep(100),
        checkin(Pool, B),
        reap_now(Pool),
        await(fun() -> not is_process_alive(A) end),
        ?assert(is_process_alive(B)),
        ?assertEqual({ready, 1, 0, 0}, poolboy:status(Pool))
    end).

idle_timeout_resets_test() ->
    with_pool([{overflow_ttl, 120}, {overflow_check_period, 1000}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        timer:sleep(80),
        ?assertEqual(B, poolboy:checkout(Pool)),
        checkin(Pool, B),
        timer:sleep(60),
        reap_now(Pool),
        ?assert(is_process_alive(B)),
        ?assertEqual({ready, 1, 1, 1}, poolboy:status(Pool)),
        timer:sleep(80),
        reap_now(Pool),
        await(fun() -> not is_process_alive(B) end),
        checkin(Pool, A)
    end).

check_period_test() ->
    with_pool([{overflow_ttl, 30}, {overflow_check_period, 300}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        timer:sleep(80),
        ?assert(is_process_alive(B)),
        await(fun() -> not is_process_alive(B) end),
        checkin(Pool, A)
    end).

default_period_test() ->
    with_pool([{overflow_ttl, 30}, {overflow_check_period, undefined}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        await(fun() -> not is_process_alive(B) end),
        checkin(Pool, A)
    end).

idle_worker_exit_test() ->
    with_pool([{overflow_ttl, 1000}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        exit(B, kill),
        await(fun() -> poolboy:status(Pool) =:= {overflow, 0, 0, 1} end),
        checkin(Pool, A),
        exit(A, kill),
        await(fun() -> poolboy:status(Pool) =:= {ready, 1, 0, 0} end),
        C = poolboy:checkout(Pool),
        ?assert(is_process_alive(C)),
        ?assert(C =/= A),
        checkin(Pool, C)
    end).

borrower_exit_test() ->
    with_pool([{overflow_ttl, 50}, {overflow_check_period, 10}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        Parent = self(),
        {Owner, Ref} = spawn_monitor(fun() ->
            Parent ! {borrowed, self(), poolboy:checkout(Pool)},
            receive stop -> ok end
        end),
        B = receive {borrowed, Owner, Worker} -> Worker after 1000 -> error(timeout) end,
        Owner ! stop,
        receive {'DOWN', Ref, process, Owner, normal} -> ok after 1000 -> error(timeout) end,
        await(fun() -> not is_process_alive(B) end),
        ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool)),
        checkin(Pool, A)
    end).

waiting_handoff_test() ->
    with_pool([{overflow_ttl, 50}, {overflow_check_period, 10}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        Parent = self(),
        {Owner, Ref} = spawn_monitor(fun() ->
            Parent ! {waiting, self()},
            Worker = poolboy:checkout(Pool),
            Parent ! {borrowed, self(), Worker},
            receive stop -> poolboy:checkin(Pool, Worker) end
        end),
        receive {waiting, Owner} -> ok after 1000 -> error(timeout) end,
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 3 end),
        checkin(Pool, B),
        receive {borrowed, Owner, B} -> ok after 1000 -> error(timeout) end,
        timer:sleep(100),
        ?assert(is_process_alive(B)),
        Owner ! stop,
        receive {'DOWN', Ref, process, Owner, normal} -> ok after 1000 -> error(timeout) end,
        await(fun() -> not is_process_alive(B) end),
        checkin(Pool, A)
    end).

checkout_timeout_test() ->
    with_pool([{overflow_ttl, 50}, {overflow_check_period, 10}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        ?assertExit({timeout, _}, poolboy:checkout(Pool, true, 10)),
        checkin(Pool, B),
        await(fun() -> not is_process_alive(B) end),
        ?assertEqual({overflow, 0, 0, 1}, poolboy:status(Pool)),
        checkin(Pool, A)
    end).

stop_test() ->
    {ok, Pool} = start_pool([{overflow_ttl, 100}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    checkin(Pool, A),
    checkin(Pool, B),
    ok = poolboy:stop(Pool),
    await(fun() -> not is_process_alive(A) andalso not is_process_alive(B) end).

legacy_state_test() ->
    with_pool([], fun(Pool) ->
        State = sys:get_state(Pool),
        Legacy = list_to_tuple(lists:sublist(tuple_to_list(State), 9)),
        ok = sys:suspend(Pool),
        sys:replace_state(Pool, fun(_) -> Legacy end),
        ok = sys:change_code(Pool, poolboy, "1.5.2", []),
        ok = sys:resume(Pool),
        ?assertEqual({ready, 1, 0, 0}, poolboy:status(Pool)),
        Worker = poolboy:checkout(Pool),
        checkin(Pool, Worker)
    end).

start_pool(Options) ->
    poolboy:start_link([{worker_module, poolboy_test_worker}, {size, 1},
                        {max_overflow, 1} | Options]).

with_pool(Options, Fun) ->
    {ok, Pool} = start_pool(Options),
    try Fun(Pool) after catch poolboy:stop(Pool) end.

checkin(Pool, Worker) ->
    ok = poolboy:checkin(Pool, Worker),
    _ = poolboy:status(Pool),
    ok.

%% Trigger the timer's normal message path for precise idle-age assertions.
reap_now(Pool) ->
    State = sys:get_state(Pool),
    Ref = element(tuple_size(State), State),
    _ = erlang:cancel_timer(Ref),
    Pool ! {timeout, Ref, reap_overflow},
    _ = poolboy:status(Pool),
    ok.

await(Predicate) ->
    await(Predicate, erlang:monotonic_time(millisecond) + 2000).

await(Predicate, Deadline) ->
    case Predicate() of
        true -> ok;
        false ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true -> timer:sleep(5), await(Predicate, Deadline);
                false -> error(condition_timeout)
            end
    end.
