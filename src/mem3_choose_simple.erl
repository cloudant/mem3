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

-module(mem3_choose_simple).

-export([get_node_info/0, choose_shards/2]).

%% includes
-include("mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

get_node_info() ->
    {ok, []}.

choose_shards(DbName, Options) ->
    Nodes = mem3:nodes(),
    NodeCount = length(Nodes),
    Suffix = couch_util:get_value(shard_suffix, Options, ""),
    N = mem3_util:n_val(couch_util:get_value(n, Options), NodeCount),
    Q = mem3_util:to_integer(couch_util:get_value(q, Options,
						  couch_config:get("cluster", "q", "8"))),
    %% rotate to a random entry in the nodelist for even distribution
    {A, B} = lists:split(crypto:rand_uniform(1,length(Nodes)+1), Nodes),
    RotatedNodes = B ++ A,
    mem3_util:create_partition_map(DbName, N, Q, RotatedNodes, Suffix).
