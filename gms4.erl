-module(gms4).

-export([start/1, start/2]).
-define(timeout, 1000).
-define(arghh, 500).
-define(ack_timeout, 500).


%% Start the first node in the group.
start(Id) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Rnd,Self) end)}.


%% Initialize the first node as leader.
init(Id, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    leader(Id, Master, [], [Master],0).


%% Leader process.
leader(Id, Master, Slaves, Group, N) ->
    receive
        {mcast, Msg} ->
            Slaves2 = reliable_bcast(Id, {msg, N, Msg}, Slaves),
            Master ! Msg,
            case Slaves2 of
                Slaves ->
                    leader(Id, Master, Slaves, Group, N+1);
                _ ->
                    Group2 = filter_group(Slaves, Slaves2, Group),
                    ViewN = N+1,
                    Slaves3 = reliable_bcast(Id, {view, ViewN, [self()|Slaves2], Group2}, Slaves2),
                    Master ! {view, Group2},
                    leader(Id, Master, Slaves3, Group2, ViewN+1)
            end;

        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            Slaves3 = reliable_bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            case Slaves3 of
                Slaves2 ->
                    leader(Id, Master, Slaves2, Group2, N+1);
                _ ->
                    Group3 = filter_group(Slaves2, Slaves3, Group2),
                    ViewN = N+1,
                    Slaves4 = reliable_bcast(Id, {view, ViewN, [self()|Slaves3], Group3}, Slaves3),
                    Master ! {view, Group3},
                    leader(Id, Master, Slaves4, Group3, ViewN+1)
            end;

        stop -> ok
    end.


%% Slave process.
slave(Id, Master, Leader, Slaves, Group, N,Last) ->
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group, N,Last);

        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group, N,Last);

        {msg,N,Msg} ->
            Leader ! {ack, N, self()},
            Master ! Msg,
            slave(Id,Master,Leader,Slaves,Group,N+1,{msg,N,Msg});

        {view,N,[Leader|Slaves2],Group2} ->
            Leader ! {ack, N, self()},
            Master ! {view,Group2},
            slave(Id,Master,Leader,Slaves2,Group2,N+1,
                {view,N,[Leader|Slaves2],Group2});

        {msg,I,_} when I < N ->
            Leader ! {ack, I, self()},
            slave(Id,Master,Leader,Slaves,Group,N,Last);

        {view,I,_,_} when I < N ->
            Leader ! {ack, I, self()},
            slave(Id,Master,Leader,Slaves,Group,N,Last);

        {'DOWN', _Ref, process, Leader, _Reason} ->
            election(Id, Master, Slaves, Group, N,Last);

        stop -> ok
    end.


%% Start a node that joins an existing group.
start(Id, Grp) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Rnd,Self) end)}.


%% Initialize a node that joins an existing group.
init(Id, Grp, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    Self = self(),
    Grp ! {join, Master, Self},
    receive
        {view, N, [Leader|Slaves], Group} ->
            erlang:monitor(process, Leader),
            Leader ! {ack, N, Self},
            Master ! {view, Group},
            slave(Id, Master, Leader, Slaves, Group, N+1,{view, N, [Leader|Slaves], Group})
    after ?timeout ->
        Master ! {error, "no reply from the leader"}
    end.


%% Reliable broadcast: sends Msg to all Nodes and waits for ACKs.
%% Monitors each node to detect crashes. Retransmits on timeout.
%% Returns the list of nodes that ACKed.
reliable_bcast(_Id, _Msg, []) ->
    [];
reliable_bcast(Id, Msg, Nodes) ->
    Refs = [{Node, erlang:monitor(process, Node)} || Node <- Nodes],
    lists:foreach(fun(Node) -> Node ! Msg, crash(Id) end, Nodes),
    SeqN = get_seq(Msg),
    Result = wait_for_acks(Id, Msg, Nodes, [], SeqN),
    lists:foreach(fun({_, Ref}) -> erlang:demonitor(Ref, [flush]) end, Refs),
    Result.

get_seq({msg, N, _}) -> N;
get_seq({view, N, _, _}) -> N.

wait_for_acks(_Id, _Msg, [], Acked, _SeqN) ->
    lists:reverse(Acked);
wait_for_acks(Id, Msg, Pending, Acked, SeqN) ->
    receive
        {ack, SeqN, From} ->
            case lists:member(From, Pending) of
                true ->
                    Pending2 = lists:delete(From, Pending),
                    wait_for_acks(Id, Msg, Pending2, [From|Acked], SeqN);
                false ->
                    wait_for_acks(Id, Msg, Pending, Acked, SeqN)
            end;
        {'DOWN', _Ref, process, Pid, _Reason} ->
            Pending2 = lists:delete(Pid, Pending),
            wait_for_acks(Id, Msg, Pending2, Acked, SeqN)
    after ?ack_timeout ->
        lists:foreach(fun(Node) -> Node ! Msg end, Pending),
        wait_for_acks(Id, Msg, Pending, Acked, SeqN)
    end.


%% Broadcast a message to all group processes in the list.
bcast(Id, Msg, Nodes) ->
    lists:foreach(
        fun(Node) ->
            Node ! Msg,
            crash(Id)
        end,
        Nodes
    ).

crash(Id) ->
    case random:uniform(?arghh) of
        ?arghh ->
            io:format("leader ~w: crash~n", [Id]),
            exit(no_luck);
        _ ->
            ok
    end.


election(Id,Master,Slaves,[_|Group],N,Last) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            Rest2 = reliable_bcast(Id,Last,Rest),
            Group2 = filter_group(Rest, Rest2, Group),
            Rest3 = reliable_bcast(Id,{view,N,[Self|Rest2],Group2},Rest2),
            Group3 = filter_group(Rest2, Rest3, Group2),
            Master ! {view,Group3},
            leader(Id,Master,Rest3,Group3,N+1);
        [Leader|Rest] ->
            erlang:monitor(process,Leader),
            slave(Id,Master,Leader,Rest,Group,N,Last)
    end.


%% Filter the group list to keep only workers whose slave survived.
filter_group(OrigSlaves, SurvivingSlaves, [Master|Workers]) ->
    Filtered = lists:filtermap(
        fun({Slave, Worker}) ->
            case lists:member(Slave, SurvivingSlaves) of
                true -> {true, Worker};
                false -> false
            end
        end,
        lists:zip(OrigSlaves, Workers)
    ),
    [Master | Filtered].
