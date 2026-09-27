-module(gms3).

-export([start/1, start/2]).
-define(timeout, 1000).
-define(arghh, 500).


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
            bcast(Id, {msg, N, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, Slaves, Group, N+1);

        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),
            bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),
            Master ! {view, Group2},
            leader(Id, Master, Slaves2, Group2, N+1);

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
            Master ! Msg,
            slave(Id,Master,Leader,Slaves,Group,N+1,{msg,N,Msg});

        {view,N,[Leader|Slaves2],Group2} ->
            Master ! {view,Group2},
            slave(Id,Master,Leader,Slaves2,Group2,N+1,
                {view,N,[Leader|Slaves2],Group2});

        {msg,I,_} when I < N ->
            slave(Id,Master,Leader,Slaves,Group,N,Last);

        {view,I,_,_} when I < N ->
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
            Master ! {view, Group},
            slave(Id, Master, Leader, Slaves, Group, N+1,{view, N, [Leader|Slaves], Group})
    after ?timeout ->
        Master ! {error, "no reply from the leader"}
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
            bcast(Id,Last,Rest),
            bcast(Id,{view,N,Slaves,Group},Rest),
            Master ! {view,Group},
            leader(Id,Master,Rest,Group,N+1);
        [Leader|Rest] ->
            erlang:monitor(process,Leader),
            slave(Id,Master,Leader,Rest,Group,N,Last)
    end.