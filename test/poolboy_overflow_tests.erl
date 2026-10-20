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
            await_status(Pool, {overflow, 0, 0, 1}),
            checkin(Pool, A)
        end).

immediate_reaping_test_() ->
    [{Name, fun() -> with_pool(Opts, fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        await(fun() -> not is_process_alive(B) end),
        await_status(Pool, {overflow, 0, 0, 1}),
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
        await_status(Pool, {ready, 1, 0, 0})
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
        await(fun() ->
            poolboy:status(Pool) =:= {ready, 1, 0, 0} andalso
                not queue:member(A, gen_server:call(Pool, get_avail_workers))
        end),
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
        await_status(Pool, {overflow, 0, 0, 1}),
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
        await_status(Pool, {overflow, 0, 0, 1}),
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

checked_out_worker_exit_test() ->
    with_pool([{overflow_ttl, 1000}], fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        exit(B, kill),
        await(fun() -> poolboy:status(Pool) =:= {overflow, 0, 0, 1} end),
        exit(A, kill),
        await(fun() -> poolboy:status(Pool) =:= {ready, 1, 0, 0} end)
    end).

pool_size_bounds_test_() ->
    [{Name, fun() ->
        {ok, Pool} = poolboy:start_link([{worker_module, poolboy_test_worker},
            {size, Size}, {max_overflow, Max}, {overflow_ttl, 30},
            {overflow_check_period, 10}]),
        try
            Workers = [poolboy:checkout(Pool) || _ <- lists:seq(1, Size + Max)],
            [checkin(Pool, W) || W <- Workers],
            await(fun() ->
                {_, Available, Overflow, Busy} = poolboy:status(Pool),
                Available =:= Size andalso Overflow =:= 0 andalso Busy =:= 0
            end),
            ?assertEqual(Size, length([W || W <- Workers, is_process_alive(W)]))
        after poolboy:stop(Pool) end
    end} || {Name, Size, Max} <- [{"multiple excess workers", 2, 3},
                                {"zero base size", 0, 2},
                                {"overflow disabled", 2, 0}]].

invalid_options_test_() ->
    [{lists:flatten(io_lib:format("~p", [Option])), fun() ->
        ?assertEqual({error, {invalid_option, Option}},
            poolboy:start([Option, {worker_module, poolboy_test_worker}]))
    end} || Option <- [{overflow_ttl, -1}, {overflow_ttl, invalid},
                       {overflow_check_period, -1}]].

zero_check_period_test_() ->
    [{Name, fun() -> with_pool(Options, fun(Pool) ->
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        checkin(Pool, B),
        case TTL of
            0 -> await(fun() -> not is_process_alive(B) end);
            _ ->
                ?assert(is_process_alive(B)),
                await(fun() -> not is_process_alive(B) end)
        end,
        checkin(Pool, A)
    end) end} || {Name, TTL, Options} <- [
        {"disabled TTL, period first", 0,
            [{overflow_check_period, 0}, {overflow_ttl, 0}]},
        {"disabled TTL, TTL first", 0,
            [{overflow_ttl, 0}, {overflow_check_period, 0}]},
        {"enabled TTL, period first", 50,
            [{overflow_check_period, 0}, {overflow_ttl, 50}]},
        {"enabled TTL, TTL first", 50,
            [{overflow_ttl, 50}, {overflow_check_period, 0}]}]].

slow_shutdown_does_not_block_checkout_test() ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 3}, {overflow_ttl, 20},
        {overflow_check_period, 50}], [{test_parent, self()}]),
    Workers = [poolboy:checkout(Pool) || _ <- lists:seq(1, 4)],
    try
        [checkin(Pool, W) || W <- Workers],
        Closing = receive {shutdown_started, W0} -> W0
                  after 1000 -> error(no_shutdown) end,
        %% A worker deliberately blocks in terminate/2 while idle capacity exists.
        Available = poolboy:checkout(Pool, true, 100),
        ?assert(Available =/= Closing),
        ?assertEqual({full, 0, 3, 1}, poolboy:status(Pool)),
        ?assertEqual(full, poolboy:checkout(Pool, false)),
        checkin(Pool, Available),
        Closing ! finish_shutdown,
        [receive {shutdown_started, W1} -> W1 ! finish_shutdown
         after 1000 -> error(no_shutdown) end || _ <- lists:seq(1, 2)],
        await(fun() -> poolboy:status(Pool) =:= {ready, 1, 0, 0} end)
    after
        [exit(W, kill) || W <- Workers],
        catch poolboy:stop(Pool)
    end.

waiting_during_shutdown_test() ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 3}, {overflow_ttl, 20},
        {overflow_check_period, 50}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    D = poolboy:checkout(Pool),
    E = poolboy:checkout(Pool),
    checkin(Pool, B),
    checkin(Pool, D),
    checkin(Pool, E),
    receive {shutdown_started, B} -> ok after 1000 -> error(no_shutdown) end,
    Parent = self(),
    {Owner, Ref} = spawn_monitor(fun() ->
        poolboy:transaction(Pool, fun(W) ->
            Parent ! {borrowed_after_shutdown, W},
            receive release -> ok end
        end)
    end),
    try
        %% The reaper, current borrower, and waiting borrower are all monitored.
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 3 end),
        B ! finish_shutdown,
        C = receive {borrowed_after_shutdown, Worker} -> Worker
            after 1000 -> error(waiter_not_woken) end,
        ?assert(is_process_alive(C)),
        ?assert(C =/= B),
        ?assertEqual({full, 0, 3, 2}, poolboy:status(Pool)),
        exit(C, kill),
        Owner ! release,
        receive {'DOWN', Ref, process, Owner, normal} -> ok
        after 1000 -> error(borrower_not_stopped) end,
        [receive {shutdown_started, W} -> W ! finish_shutdown
         after 1000 -> error(no_shutdown) end || _ <- lists:seq(1, 2)],
        await_status(Pool, {overflow, 0, 0, 1})
    after
        exit(Owner, kill),
        exit(A, kill),
        exit(B, kill),
        exit(D, kill),
        exit(E, kill),
        catch poolboy:stop(Pool)
    end.

stop_during_shutdown_test() ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 1}, {overflow_ttl, 20},
        {overflow_check_period, 50}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    try
        checkin(Pool, A),
        checkin(Pool, B),
        Closing = receive {shutdown_started, Worker} -> Worker
                  after 1000 -> error(no_shutdown) end,
        [{process, Reaper}] = element(2, process_info(Pool, monitors)),
        ?assert(is_process_alive(Reaper)),
        ok = poolboy:stop(Pool),
        await(fun() -> not is_process_alive(Reaper) end),
        Closing ! finish_shutdown
    after
        exit(A, kill),
        exit(B, kill),
        catch poolboy:stop(Pool)
    end.

killed_pool_stops_reaper_test() ->
    {ok, Pool} = poolboy:start([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 1}, {overflow_ttl, 20},
        {overflow_check_period, 50}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    checkin(Pool, B),
    receive {shutdown_started, B} -> ok after 1000 -> error(no_shutdown) end,
    [Reaper] = [P || {process, P} <- element(2, process_info(Pool, monitors)), P =/= self()],
    try
        %% Hold the pool before it can acknowledge the completed shutdown.
        ok = sys:suspend(Pool),
        B ! finish_shutdown,
        await(fun() -> lists:member({overflow_reaped, Reaper, B},
            element(2, process_info(Pool, messages))) end),
        exit(Pool, kill),
        await(fun() -> not is_process_alive(Pool) end),
        await(fun() -> not is_process_alive(Reaper) end)
    after
        exit(Pool, kill),
        exit(Reaper, kill),
        exit(A, kill),
        exit(B, kill)
    end.

idle_overflow_timer_lifecycle_test() ->
    with_pool([{overflow_ttl, 80}, {overflow_check_period, 100}], fun(Pool) ->
        ?assertEqual(undefined, overflow_timer(Pool)),
        A = poolboy:checkout(Pool),
        B = poolboy:checkout(Pool),
        ?assertEqual(undefined, overflow_timer(Pool)),
        checkin(Pool, B),
        OldRef = overflow_timer(Pool),
        ?assert(is_reference(OldRef)),
        ?assertEqual(B, poolboy:checkout(Pool)),
        ?assertEqual(undefined, overflow_timer(Pool)),
        checkin(Pool, B),
        NewRef = overflow_timer(Pool),
        ?assert(is_reference(NewRef)),
        ?assert(OldRef =/= NewRef),
        Pool ! {timeout, OldRef, reap_overflow},
        ?assertEqual(NewRef, overflow_timer(Pool)),
        await_status(Pool, {overflow, 0, 0, 1}),
        ?assertEqual(undefined, overflow_timer(Pool)),
        checkin(Pool, A),
        ?assertEqual(undefined, overflow_timer(Pool))
    end).

no_overflow_timer_test_() ->
    [{Name, fun() -> with_pool(Options, fun(Pool) ->
        ?assertEqual(undefined, overflow_timer(Pool)),
        A = poolboy:checkout(Pool),
        checkin(Pool, A),
        ?assertEqual(undefined, overflow_timer(Pool))
    end) end} || {Name, Options} <- [
        {"TTL disabled", [{overflow_ttl, 0}, {overflow_check_period, 1}]},
        {"overflow disabled", [{overflow_ttl, 10}, {max_overflow, 0}]}]].

timer_resumes_after_reaping_test() ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 2}, {overflow_ttl, 40},
        {overflow_check_period, 10}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    C = poolboy:checkout(Pool),
    checkin(Pool, B),
    checkin(Pool, C),
    receive {shutdown_started, B} -> ok after 1000 -> error(no_shutdown) end,
    Parent = self(),
    {Owner, Ref} = spawn_monitor(fun() ->
        Worker = poolboy:checkout(Pool),
        poolboy:checkin(Pool, Worker),
        Parent ! {returned_during_reaping, Worker}
    end),
    try
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 3 end),
        B ! finish_shutdown,
        D = receive {returned_during_reaping, Worker} -> Worker
            after 1000 -> error(no_replacement) end,
        receive {'DOWN', Ref, process, Owner, normal} -> ok
        after 1000 -> error(owner_not_stopped) end,
        receive {shutdown_started, C} -> ok after 1000 -> error(no_shutdown) end,
        await_status(Pool, {ready, 1, 2, 1}),
        ?assertEqual(undefined, overflow_timer(Pool)),
        C ! finish_shutdown,
        receive {shutdown_started, D} -> D ! finish_shutdown
        after 1000 -> error(timer_not_resumed) end,
        await_status(Pool, {overflow, 0, 0, 1}),
        ?assertEqual(undefined, overflow_timer(Pool)),
        checkin(Pool, A)
    after
        exit(Owner, kill),
        catch poolboy:stop(Pool),
        exit(A, kill),
        exit(B, kill),
        exit(C, kill)
    end.

overflow_timer(Pool) ->
    State = sys:get_state(Pool),
    element(tuple_size(State), State).

checkout_below_limit_during_shutdown_test() ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 2}, {overflow_ttl, 20},
        {overflow_check_period, 10}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    checkin(Pool, B),
    receive {shutdown_started, B} -> ok after 1000 -> error(no_shutdown) end,
    Parent = self(),
    {Owner, Ref} = spawn_monitor(fun() ->
        poolboy:transaction(Pool, fun(W) ->
            Parent ! {borrowed_below_limit, W},
            receive release -> ok end
        end, 1000)
    end),
    try
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 3 end),
        ?assertEqual({full, 0, 1, 1}, gen_server:call(Pool, status, 100)),
        ?assertEqual(full, poolboy:checkout(Pool, false, 100)),
        checkin(Pool, A),
        receive {borrowed_below_limit, A} -> ok
        after 500 -> error(checkin_not_processed) end,
        Owner ! release,
        receive {'DOWN', Ref, process, Owner, normal} -> ok
        after 1000 -> error(owner_not_done) end,
        B ! finish_shutdown,
        await_status(Pool, {ready, 1, 0, 0})
    after
        exit(Owner, kill),
        exit(A, kill), exit(B, kill),
        catch poolboy:stop(Pool)
    end.

replacement_during_shutdown_test_() ->
    [{atom_to_list(Mode), fun() -> replacement_during_shutdown(Mode) end}
     || Mode <- [borrowed, idle]].

replacement_during_shutdown(Mode) ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 2}, {max_overflow, 1}, {overflow_ttl, 20},
        {overflow_check_period, 10}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    C = poolboy:checkout(Pool),
    checkin(Pool, C),
    receive {shutdown_started, C} -> ok after 1000 -> error(no_shutdown) end,
    try
        %% Losing two workers requires restoring the base pool, while
        %% the supervisor is still busy terminating C.
        case Mode of
            borrowed -> ok;
            idle -> checkin(Pool, B)
        end,
        exit(A, kill),
        case Mode of
            borrowed -> await_status(Pool, {full, 0, 0, 1});
            idle -> await_status(Pool, {ready, 1, 0, 0})
        end,
        exit(B, kill),
        await_status(Pool, {full, 0, 0, 0}),
        ?assertEqual({full, 0, 0, 0}, gen_server:call(Pool, status, 100)),
        C ! finish_shutdown,
        await_status(Pool, {ready, 2, 0, 0}),
        D = poolboy:checkout(Pool),
        E = poolboy:checkout(Pool),
        ?assert(is_process_alive(D)), ?assert(is_process_alive(E)),
        ?assert(D =/= E),
        checkin(Pool, D), checkin(Pool, E)
    after
        exit(A, kill), exit(B, kill), exit(C, kill),
        catch poolboy:stop(Pool)
    end.

cancel_deferred_checkout_test_() ->
    [{atom_to_list(Mode), fun() -> cancel_deferred_checkout(Mode) end}
     || Mode <- [timeout, owner_exit]].

cancel_deferred_checkout(Mode) ->
    {ok, Pool} = poolboy:start_link([{worker_module, poolboy_shutdown_worker},
        {size, 1}, {max_overflow, 2}, {overflow_ttl, 20},
        {overflow_check_period, 10}], [{test_parent, self()}]),
    A = poolboy:checkout(Pool),
    B = poolboy:checkout(Pool),
    checkin(Pool, B),
    receive {shutdown_started, B} -> ok after 1000 -> error(no_shutdown) end,
    Parent = self(),
    {Owner, Ref} = spawn_monitor(fun() ->
        Result = catch poolboy:checkout(Pool, true, 200),
        Parent ! {cancelled_during_shutdown, self(), Result},
        receive stop -> ok end
    end),
    try
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 3 end),
        case Mode of
            timeout ->
                receive {cancelled_during_shutdown, Owner, {'EXIT', {timeout, _}}} -> ok
                after 1000 -> error(no_timeout) end;
            owner_exit ->
                exit(Owner, kill),
                receive {'DOWN', Ref, process, Owner, killed} -> ok
                after 1000 -> error(owner_not_done) end
        end,
        await(fun() -> length(element(2, process_info(Pool, monitors))) =:= 2 end),
        B ! finish_shutdown,
        await_status(Pool, {overflow, 0, 0, 1}),
        ?assertEqual(1, length(gen_server:call(Pool, get_all_workers)))
    after
        exit(Owner, kill),
        exit(A, kill), exit(B, kill),
        catch poolboy:stop(Pool)
    end.

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

await_status(Pool, Status) ->
    await(fun() -> poolboy:status(Pool) =:= Status end).

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
