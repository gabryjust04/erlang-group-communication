-module(gms2).

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
    leader(Id, Master, [], [Master]).


%% Leader process.
leader(Id, Master, Slaves, Group) ->
    receive

        %% Multicast a message to all members.
        {mcast, Msg} ->
            bcast(Id, {msg, Msg}, Slaves),
            Master ! Msg,
            leader(Id, Master, Slaves, Group);

        %% Add a new node to the group.
        {join, Wrk, Peer} ->
            Slaves2 = lists:append(Slaves, [Peer]),
            Group2 = lists:append(Group, [Wrk]),

            bcast(
                Id,
                {view, [self() | Slaves2], Group2},
                Slaves2
            ),

            Master ! {view, Group2},

            leader(Id, Master, Slaves2, Group2);

        %% Stop the group process.
        stop ->
            ok
    end.


%% Slave process.
slave(Id, Master, Leader, Slaves, Group) ->
    receive

        %% Forward multicast requests to the leader.
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group);

        %% Forward join requests to the leader.
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group);

        %% Receive a multicasted message from the leader.
        {msg, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group);

        %% Receive a new group view.
        {view, [Leader | Slaves2], Group2} ->
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2);

        {'DOWN', _Ref,process,Leader,_Reason} -> 
            election(Id,Master,Slaves,Group);

        %% Stop the group process.
        stop ->
            ok
    end.


%% Start a node that joins an existing group.
start(Id, Grp) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Rnd,Self) end)}.


%% Initialize a node that joins an existing group.
init(Id, Grp, Rnd,Master) ->
    random:seed(Rnd, Rnd, Rnd),
    Self = self(),

    Grp ! {join, Master, Self},

    receive
        {view, [Leader | Slaves], Group} ->
            erlang:monitor(process, Leader),
            Master ! {view, Group},
            slave(Id, Master, Leader, Slaves, Group)
        after ?timeout ->
            Master ! {error,"no reply from the leader"}
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


election(Id,Master,Slaves,[_ |Group]) ->
    Self = self(),
    case Slaves of
        [Self | Rest] ->
            bcast(Id,{view,Slaves,Group},Rest),
            Master ! {view,Group},
            leader(Id,Master,Rest,Group);
        [Leader | Rest] ->
            erlang:monitor(process,Leader),
            slave(Id,Master,Leader,Rest,Group) end.