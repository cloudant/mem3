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
    Zones = zones(Nodes),
    Suffix = couch_util:get_value(shard_suffix, Options, ""),
    Q = erlang:max(1, proplists:get_value(q, Options, 8)),
    N = erlang:min(length(Nodes), proplists:get_value(n, Options, 3)),
    Z = erlang:min(length(Zones), proplists:get_value(z, Options, 3)),
    Zones1 = lists:sublist(shuffle(Zones), Z),
    Nodes1 = [N1 || N1 <- Nodes, lists:member(mem3:node_info(zone, <<"zone">>), Zones1)],
    lists:usort(mem3_util:create_partition_map(DbName, N, Q, Nodes1, Suffix)).

zones(Nodes) ->
    lists:usort([mem3:node_info(Node, <<"zone">>) || Node <- Nodes]).

shuffle(List) ->
    %% Determine the log n portion then randomize the list.
    randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
    randomize(List);
randomize(T, List) ->
    lists:foldl(fun(_E, Acc) ->
                        randomize(Acc)
                end, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
    D = lists:map(fun(A) ->
                          {random:uniform(), A}
                  end, List),
    {_, D1} = lists:unzip(lists:keysort(1, D)),
    D1.
