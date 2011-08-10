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

% Determine liveness of nodes.

-module(mem3_live).
-behaviour(gen_server).

-define(HEARTBEAT_INTERVAL, 5000).
-define(LIVENESS_THRESHOLD, 30000000).

% public api
-export([start_link/0, live_nodes/0, ping/0]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
    
live_nodes() ->
    gen_server:call(?MODULE, live_nodes).

ping() ->
    rpc:abcast(couch_server, {ping, whereis(?MODULE)}).

init(_) ->
    net_kernel:monitor_nodes(true),
    ?MODULE = ets:new(?MODULE, [set, protected, named_table]),
    TRef = timer:apply_interval(?HEARTBEAT_INTERVAL, ?MODULE, ping, []),
    {ok, TRef}.

handle_call(live_nodes, _From, State) ->
    Now = now(),
    Live = [Node || {Node, LastPing} <- ets:tab2list(?MODULE),
        timer:now_diff(Now, LastPing) < ?LIVENESS_THRESHOLD],
    {reply, Live, State}.

handle_cast(Msg, Server) ->
    {stop, {unexpected_msg, Msg}, Server}.

handle_info({nodedown, Node}, State) ->
    ets:delete(?MODULE, Node),
    {noreply, State};
handle_info({pong, Node}, State) ->
    ets:insert(?MODULE, {Node, now()}),
    {noreply, State}.

terminate(_Reason, TRef) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


