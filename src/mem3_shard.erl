% Copyright 2013 Cloudant
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

%% Use these methods when a received #shard record might be in the old format.

-module(mem3_shard).

-export([name/1,
         node/1,
         dbname/1,
         range/1,
         ref/1,
         order/1,
         set_ref/2]).

-include("mem3.hrl").

name(Shard)         -> (upgrade_shard(Shard))#shard.name.
node(Shard)         -> (upgrade_shard(Shard))#shard.node.
dbname(Shard)       -> (upgrade_shard(Shard))#shard.dbname.
range(Shard)        -> (upgrade_shard(Shard))#shard.range.
ref(Shard)          -> (upgrade_shard(Shard))#shard.ref.
order(Shard)        -> (upgrade_shard(Shard))#shard.order.
set_ref(Shard, Ref) -> (upgrade_shard(Shard))#shard{ref=Ref}.

upgrade_shard(#shard{}=S) -> S;
upgrade_shard({Name, Node, DbName, Range, Ref}) ->
    #shard{name=Name, node=Node, dbname=DbName, range=Range, ref=Ref}.
