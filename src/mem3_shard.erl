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
         upgrade/1,
         set_name/2,
         set_node/2,
         set_dbname/2,
         set_ref/2]).

-include("mem3.hrl").

name(Shard)               -> (upgrade(Shard))#shard.name.
node(Shard)               -> (upgrade(Shard))#shard.node.
dbname(Shard)             -> (upgrade(Shard))#shard.dbname.
range(Shard)              -> (upgrade(Shard))#shard.range.
ref(Shard)                -> (upgrade(Shard))#shard.ref.
order(Shard)              -> (upgrade(Shard))#shard.order.
set_ref(Shard, Ref)       -> (upgrade(Shard))#shard{ref=Ref}.
set_dbname(Shard, DbName) -> (upgrade(Shard))#shard{dbname=DbName}.
set_node(Shard, Node)     -> (upgrade(Shard))#shard{node=Node}.
set_name(Shard, Name)     -> (upgrade(Shard))#shard{name=Name}.

upgrade(#shard{}=S) -> S;
upgrade({shard, Name, Node, DbName, Range, Ref}) ->
    #shard{name=Name, node=Node, dbname=DbName, range=Range, ref=Ref}.
