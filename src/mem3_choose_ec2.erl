%% Copyright 2011 Cloudant
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(mem3_choose_ec2).

-export([get_node_info/0, choose_shards/2]).

%% includes
-include("mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

get_node_info() ->
    case ibrowse:send_req("http://169.254.169.254/2011-01-01" ++
                              "/meta-data/placement/availability-zone",
                          [], get, <<>>, [{connect_timeout, 2000}]) of
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
    N = mem3_util:n_val(couch_util:get_value(n, Options), NodeCount),
    Q = mem3_util:to_integer(couch_util:get_value(q, Options,
                                                  couch_config:get("cluster", "q", "8"))),
    Z = mem3_util:z_val(couch_util:get_value(z, Options), NodeCount, ZoneCount),
    Suffix = couch_util:get_value(shard_suffix, Options, ""),

    ChosenZones = lists:sublist(shuffle(Zones), Z),
    lists:flatmap(
      fun({Zone, N1}) ->
              Nodes1 = nodes_in_zone(Nodes, Zone),
              {A, B} = lists:split(crypto:rand_uniform(1,length(Nodes1)+1), Nodes1),
              RotatedNodes = B ++ A,
              mem3_util:create_partition_map(DbName, erlang:min(N1,length(Nodes1)),
                                             Q, RotatedNodes, Suffix)
      end,
      lists:zip(ChosenZones, apportion(N, Z))).

zones(Nodes) ->
    lists:usort([mem3:node_info(Node, <<"zone">>) || Node <- Nodes]).

nodes_in_zone(Nodes, Zone) ->
    [Node || Node <- Nodes, Zone == mem3:node_info(Node, <<"zone">>)].

shuffle(List) ->
    %% Determine the log n portion then randomize the list.
    randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
    randomize(List);
randomize(T, List) ->
    lists:foldl(fun(_E, Acc) -> randomize(Acc) end,
                randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
    D = lists:map(fun(A) -> {random:uniform(), A} end, List),
    {_, D1} = lists:unzip(lists:keysort(1, D)),
    D1.

apportion(Shares, Ways) ->
    apportion(Shares, lists:duplicate(Ways, 0), Shares).

apportion(_Shares, Acc, 0) ->
    Acc;
apportion(Shares, Acc, Remaining) ->
    N = Remaining rem length(Acc),
    [H|T] = lists:nthtail(N, Acc),
    apportion(Shares, lists:sublist(Acc, N) ++ [H+1|T], Remaining - 1).
