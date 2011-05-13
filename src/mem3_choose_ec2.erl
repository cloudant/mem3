% Copyright 2011 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(mem3_choose_ec2).

-export([get_node_info/0, choose_shards/2]).

%% includes
-include("mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

get_node_info() ->
    case ibrowse:send_req("http://169.254.169.254/2011-01-01" ++
        "/meta-data/placement/availability-zone", [], get) of
        {ok, "200", _, Body} ->
            {ok, [{zone, Body}]};
        _ ->
            {ok, []}
    end.

choose_shards(DbName, Options) ->
    Nodes = mem3:nodes(),
    NodeCount = length(Nodes),
    Zones = zones(Nodes),
    ZoneCount = length(Zones),
    Suffix = couch_util:get_value(shard_suffix, Options, ""),
    N = mem3_util:n_val(couch_util:get_value(n, Options), NodeCount),
    Q = mem3_util:to_integer(couch_util:get_value(q, Options,
						  couch_config:get("cluster", "q", "8"))),
    Z = mem3_util:z_val(couch_util:get_value(z, Options), NodeCount, ZoneCount),

    ChosenZones = lists:sublist(random_rotate(zones(Nodes)), Z),
    ChosenNodes = lists:flatmap(fun(Zone) -> pad(nodes(Nodes, Zone), Q) end, ChosenZones),
    ChosenNodes0 = random_rotate(pad(ChosenNodes, N * Q)),
    UniqueShards = mem3_util:make_key_ranges(Q),
    Shards0 = duplicate(UniqueShards, N),
    Map = lists:zipwith(fun(N2, S2) -> S2#shard{node=N2} end, ChosenNodes0, Shards0),
    lists:keysort(#shard.range, [mem3_util:name_shard(S#shard{dbname=DbName}, Suffix) || S <- Map]).

zones(Nodes) ->
    lists:usort([mem3:node_info(Node, <<"zone">>) || Node <- Nodes]).

nodes(Nodes, Zone) ->
    [Node || Node <- Nodes, Zone == mem3:node_info(Node, <<"zone">>)].

random_rotate(List) ->
    {A, B} = lists:split(crypto:rand_uniform(1,length(List)+1), List),
    B ++ A.

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
