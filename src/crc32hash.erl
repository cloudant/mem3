% Copyright 2012 Cloudant
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

-module(crc32hash).
-behaviour(mem3_hash).

-export([mem3_hash/2]).

-include_lib("couch/include/couch_db.hrl").

mem3_hash(DbName, Doc) when is_record(Doc, doc) ->
  mem3_hash(DbName, Doc#doc.id); 

mem3_hash(_DbName, DocId) when is_binary(DocId) ->
  {default, erlang:crc32(DocId)};

mem3_hash(_DbName, DocId) ->
  erlang:crc32(term_to_binary((DocId))).
