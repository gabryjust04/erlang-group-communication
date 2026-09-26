-module(gms1).

-export([start/1, start/2]).


%% Start the first node in the group.
start(Id) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Self) end)}.


%% Initialize the first node as leader.
init(Id, Master) ->
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

        %% Stop the group process.
        stop ->
            ok
    end.


%% Start a node that joins an existing group.
start(Id, Grp) ->
    Self = self(),
    {ok, spawn_link(fun() -> init(Id, Grp, Self) end)}.


%% Initialize a node that joins an existing group.
init(Id, Grp, Master) ->
    Self = self(),

    Grp ! {join, Master, Self},

    receive
        {view, [Leader | Slaves], Group} ->
            Master ! {view, Group},
            slave(Id, Master, Leader, Slaves, Group)
    end.


%% Broadcast a message to all group processes in the list.
bcast(_Id, _Msg, []) ->
    ok;

bcast(Id, Msg, [Receiver | Slaves]) ->
    Receiver ! Msg,
    bcast(Id, Msg, Slaves).