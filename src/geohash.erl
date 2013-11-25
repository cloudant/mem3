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

-module(geohash).
-behaviour(mem3_hash).

-export([mem3_hash/2, geohash/3, format_geohash/2]).

-include_lib("couch/include/couch_db.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(bbox, {minx=-180, miny=-90, maxx=180, maxy=90}).

-define(GEOHASH, "0123456789bcdefghjkmnpqrstuvwxyz").
-define(TYPE, geo).

mem3_hash(DbName, Doc) when is_record(Doc, doc) ->
  case get_xycenter(Doc#doc.body) of
  {X, Y} ->
     % get precision from config, this is fixed when the database is created
     % 10 char precision can be achieved with 50 bits, so 1 bsl 64 is a good ringtop
     RingTop = mem3_util:ringtop(DbName), 
     Precision = ((byte_size(binary:encode_unsigned(RingTop)) - 1) * 8) div 5,
     Hash = geohash(X, Y, Precision),
     {?TYPE, Hash};
  null ->
    throw({bad_request, <<"Document must be in geojson format">>})
  end;


mem3_hash(DbName, <<"foo">>) ->
  % do nothing, this is a dummy run to calculate number of shards in database
  {?TYPE, 0};

mem3_hash(DbName, DocId) ->
  Id = ?b2l(DocId),
  case string:tokens(Id, "-") of
  [_, Hash] ->
    list_to_integer(Hash);
  _ ->
    throw({bad_request, <<"Document not found">>})
  end.  

geohash(X, Y, _Precision) when (X > 180) or (X < -180) or (Y < -90) or (Y > 90) ->
  throw({bad_request, lists:flatten(io_lib:format("Geohash Lat/Lon ~p, ~p out of bounds", [Y, X]))});

% precision is required because it defines the pad
geohash(X, Y, Precision) ->
  geohash(X, Y, 5 * Precision, lon, #bbox{}, <<>>, Precision).

geohash(_X, _Y, 0, _, _Bbox, Bin, Precision) ->
  Pad = (byte_size(Bin) * 8) - (5 * Precision),
  % return number not hash
  binary:decode_unsigned(<<0:Pad, Bin/bits>>, big);

geohash(X, Y, Counter, lon, #bbox{minx=MinX, maxx=MaxX} = Bbox, Bin, Precision) ->
  MidX = (MinX + MaxX) / 2,
 	case (X > MidX) of
 	true -> 
    geohash(X, Y, Counter - 1, lat, Bbox#bbox{minx=MidX}, <<Bin/bits, 1:1>>, Precision);
 	_ ->
    geohash(X, Y, Counter - 1, lat, Bbox#bbox{maxx=MidX}, <<Bin/bits, 0:1>>, Precision)
 	end;

geohash(X, Y, Counter, lat,  #bbox{miny=MinY, maxy=MaxY} = Bbox, Bin, Precision) ->
	 MidY = (MinY + MaxY) / 2,
	 case (Y > MidY) of
	 true ->
		  geohash(X, Y, Counter - 1, lon, Bbox#bbox{miny=MidY}, <<Bin/bits, 1:1>>, Precision);
	 _ ->
		  geohash(X, Y, Counter - 1, lon, Bbox#bbox{maxy=MidY}, <<Bin/bits, 0:1>>, Precision)
	 end.

format_geohash(Val, Precision) ->
  % remove or add padding
  Bin = binary:encode_unsigned(Val, big),
  BinSize = byte_size(Bin),
  NewBin = case (BinSize * 8) < (5 * Precision) of 
  true ->
    % add missing 0 bits to Bin
    Pad = (5 * Precision) - (8 * BinSize),
    <<0:Pad, Bin/bits>>;
  _ ->
    Pad = (8 * BinSize) - (5 * Precision),
    <<0:Pad, Rem/bits>> = Bin,
    Rem
  end,
  format_geohash(NewBin, [], Precision).

% private
get_xycenter({Doc}) ->
  case lists:keyfind(<<"bbox">>, 1, Doc) of
  {_, [MinX, MinY, MaxX, MaxY]} ->
    {(MinX + MaxX) / 2, (MinY + MaxY) / 2};
  _ ->
    % process other simple geometry types
    case lists:keyfind(<<"geometry">>, 1, Doc) of 
      {_, {[{<<"type">>, GeomType}, {<<"coordinates">>, Coords}]}} ->
        get_xycenter(GeomType, Coords);
      _ ->
        null
    end
  end.

get_xycenter(<<"Point">>, [X, Y]) ->
  {X, Y};

get_xycenter(_, _) ->
  null.

format_geohash(<<>>, Acc, _Precision) ->
  lists:flatten(lists:reverse(Acc));

format_geohash(<<V:5, Rem/bits>>, Acc, Precision) ->
  % format hash as base 32
  C = string:substr(?GEOHASH, V + 1, 1),
  format_geohash(Rem, [C|Acc], Precision).

-ifdef(TEST).
% local tests for geohashing
hash_test() ->
  Precision = 5,
  % test error handling in lon and lat
  ?assertException(throw, {bad_request,_}, geohash(-181, 0, Precision)),
  ?assertException(throw, {bad_request,_}, geohash(0, -91, Precision)),   
  ?assertException(throw, {bad_request,_}, geohash(181, 0, Precision)),
  ?assertException(throw, {bad_request,_}, geohash(0, 91, Precision)),   

  % wikipedia worked example, hash should yield ezs42
  ?assertEqual("ezs42",
    format_geohash(
        geohash(-5.6, 42.6, Precision)
        , Precision)).

center_test() ->
  ?assertEqual({0.0, 0.0}, get_xycenter({[{<<"bbox">>, [-180, -90, 180, 90]}]})),
  ?assertEqual({0.0, 0.0}, get_xycenter({[{<<"geometry">>,{[{<<"type">>,<<"Point">>},{<<"coordinates">>,[0.0,0.0]}]}}]})).
-endif.
