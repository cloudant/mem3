#!/usr/bin/env escript

-record(mynode, {name, zone}).
-define(RINGTOP, 2 bsl 31).  % CRC32 space

-record(shard, {
    name :: binary() | '_',
    node :: node() | '_',
    dbname :: binary(),
    range :: [non_neg_integer() | '$1' | '$2'],
    ref :: reference() | 'undefined' | '_'
}).

main(_) ->
    DbName="foo",
    N = 2,
    Q = 3,
    Z = 3,
    Suffix = ".suff",
    Nodes = lists:foldl(fun(Num, Acc) ->
				Name = format("node-~B", [Num]),
				Zone = format("zone-~B", [Num rem (Z+2)]),
				[#mynode{name=Name,zone=Zone}] ++ Acc
			end, [], lists:seq(0, 10)),
    io:format("Nodes: ~p~n", [Nodes]),
    Map = create_partition_map(DbName, N, Q, Z, Nodes, Suffix),
    io:format("Map: ~p~n", [Map]).

create_partition_map(DbName, N, Q, Z, Nodes, Suffix) ->
    Zones = lists:sublist(random_rotate(zones(Nodes)), Z),
    ChosenNodes = lists:flatmap(fun(Zone) -> pad(nodes(Nodes, Zone), Q) end, Zones),
    ChosenNodes0 = random_rotate(pad(ChosenNodes, N * Q)),
    UniqueShards = make_key_ranges((?RINGTOP) div Q, 0, []),
    Shards0 = duplicate(UniqueShards, N),
    Map = lists:zipwith(fun(N2, S2) -> S2#shard{node=N2} end, ChosenNodes0, Shards0),
    lists:keysort(#shard.range, [name_shard(S#shard{dbname=DbName}, Suffix) || S <- Map]).

zones(Nodes) ->
    [Z || #mynode{zone=Z} <- lists:ukeysort(#mynode.zone, Nodes)].

nodes(Nodes, Zone) ->
    [N || #mynode{name=N,zone=Z} <- Nodes, Z == Zone].

random_rotate(List) ->
    {A, B} = lists:split(crypto:rand_uniform(1,length(List)+1), List),
    B ++ A.

format(Format, Data) ->
    lists:flatten(io_lib:format(Format, Data)).

duplicate(List, N) ->
    duplicate(List, N, []).

duplicate(_List, 0, Acc) ->
    Acc;
duplicate(List, N, Acc) ->
    duplicate(List, N-1, List ++ Acc).

pad(List, Len) when length(List) >= Len ->
    lists:sublist(List, Len);
pad(List, Len) ->
    pad(List ++ List, Len).

%% VV unchanged from mem3_util.erl VV

make_key_ranges(_, CurrentPos, Acc) when CurrentPos >= ?RINGTOP ->
    Acc;
make_key_ranges(Increment, Start, Acc) ->
    case Start + 2*Increment of
    X when X > ?RINGTOP ->
        End = ?RINGTOP - 1;
    _ ->
        End = Start + Increment - 1
    end,
    make_key_ranges(Increment, End+1, [#shard{range=[Start, End]} | Acc]).

name_shard(#shard{dbname = DbName, range=[B,E]} = Shard, Suffix) ->
    Name = ["shards/", to_hex(<<B:32/integer>>), "-",
	    to_hex(<<E:32/integer>>), "/", DbName, Suffix],
    Shard#shard{name = list_to_binary(Name)}.

to_hex([]) ->
    [];
to_hex(Bin) when is_binary(Bin) ->
    to_hex(binary_to_list(Bin));
to_hex([H|T]) ->
    [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.
