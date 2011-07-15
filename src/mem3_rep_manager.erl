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

-module(mem3_rep_manager).

-behaviour(gen_server).

% public API
-export([start_link/0]).
-export([replication_started/1, replication_completed/1, replication_error/2]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-include("mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_js_functions.hrl").

-define(DOC_TO_REP, mem3_rep_doc_id_to_rep_id).
-define(REP_TO_STATE, mem3_rep_id_to_rep_state).
-define(DB_TO_SEQ, mem3_db_to_seq). %% remembers latest seq of _replicator db(s)
-define(INITIAL_WAIT, 2.5). % seconds
-define(MAX_WAIT, 600).     % seconds
-define(CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}}).

-record(state, {
    db_notifier = nil,
    max_retries,
    members=[],
    rep_start_pids = [],
    scan_pid = nil
}).

-record(rep_state, {
    doc_id,
    user_ctx,
    doc,
    starting,
    retries_left,
    max_retries,
    wait = ?INITIAL_WAIT
}).

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

replication_started({BaseId, _} = RepId) ->
    stub.

replication_completed(RepId) ->
    stub.

replication_error({BaseId, _} = RepId, Error) ->
    stub.

init(_) ->
    process_flag(trap_exit, true),
    net_kernel:monitor_nodes(true),
    ?DOC_TO_REP = ets:new(?DOC_TO_REP, [named_table, set, protected]),
    ?REP_TO_STATE = ets:new(?REP_TO_STATE, [named_table, set, protected]),
    ?DB_TO_SEQ = ets:new(?DB_TO_SEQ, [named_table, set, protected]),
    Server = self(),
    ok = couch_config:register(
        fun("replicator", "max_replication_retry_count", V) ->
            ok = gen_server:cast(Server, {set_max_retries, retries_value(V)})
        end
    ),
    Pid = spawn_link(fun scan_for_replication_jobs/0),
    {ok, #state{
       members = lists:sort([node()|nodes()]),
       db_notifier = db_update_notifier(),
       scan_pid = Pid,
       max_retries = retries_value(
           couch_config:get("replicator", "max_replication_retry_count", "10"))
      }}.

handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast({set_max_retries, MaxRetries}, State) ->
    {noreply, State#state{max_retries = MaxRetries}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, #state{members=Members}=State) ->
    twig:log(notice, "Node ~p came up, scanning for replication tasks to relinquish.", [Node]),
    {noreply, State#state{members=lists:sort([Node] ++ Members)}};
handle_info({nodedown, Node}, #state{members=Members}=State) ->
    twig:log(notice, "Node ~p went down, scanning for orphaned replication tasks.", [Node]),
    {noreply, State#state{members=Members -- [Node]}};
handle_info({'EXIT', From, normal}, #state{scan_pid = From} = State) ->
    twig:log(notice, "Background scan has completed.", []),
    {noreply, State#state{scan_pid=nil}};
handle_info({'EXIT', From, Reason}, #state{scan_pid = From} = State) ->
    twig:log(error, "Background scanner died. Reason: ~p", [Reason]),
    {stop, {scanner_died, Reason}, State};
handle_info({'EXIT', From, Reason}, #state{db_notifier = From} = State) ->
    twig:log(error, "Database update notifier died. Reason: ~p", [Reason]),
    {stop, {db_update_notifier_died, Reason}, State}.

terminate(_Reason, State) ->
    #state{
        scan_pid = ScanPid,
        rep_start_pids = StartPids,
        db_notifier = DbNotifier
    } = State,
    stop_all_replications(),
    lists:foreach(
        fun(Pid) ->
            catch unlink(Pid),
            catch exit(Pid, stop)
        end,
        [ScanPid | StartPids]),
    true = ets:delete(?REP_TO_STATE),
    true = ets:delete(?DOC_TO_REP),
    true = ets:delete(?DB_TO_SEQ),
    couch_db_update_notifier:stop(DbNotifier).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

is_current_owner({Change}, Members) ->
    is_current_owner(get_value(id, Change), Members);
is_current_owner(Id, Members) ->
    Hash = mem3_util:hash(Id),
    node() =:= lists:nth(1 + Hash rem length(Members), Members).

db_update_notifier() ->
    {ok, Notifier} = couch_db_update_notifier:start_link(
        fun({updated, DbName}) ->
            case is_replicator_db(DbName) of
            true ->
                twig:log(notice, "_replicator updated.",[]);
            _ ->
                ok
            end;
        ({deleted, DbName}) ->
            case is_replicator_db(DbName) of
            true ->
                twig:log(notice, "_replicator deleted.",[]);
            _ ->
                ok
            end
        end
    ),
    Notifier.

retries_value("infinity") ->
    infinity;
retries_value(Value) ->
    list_to_integer(Value).

stop_all_replications() ->
    twig:log(notice, "Stopping all ongoing replications because the replicator"
        " database was deleted or changed", []),
    ets:foldl(
        fun({_, RepId}, _) ->
            couch_replicator:cancel_replication(RepId)
        end,
        ok, ?DOC_TO_REP),
    true = ets:delete_all_objects(?REP_TO_STATE),
    true = ets:delete_all_objects(?DOC_TO_REP),
    true = ets:delete_all_objects(?DB_TO_SEQ).

scan_for_replication_jobs() ->
    {ok, Dbs} = do_async(fabric, all_dbs, []),
    scan_for_replication_jobs(Dbs).

scan_for_replication_jobs([]) ->
    ok;
scan_for_replication_jobs([Db|Rest]) ->
    case is_replicator_db(Db) of
        true -> scan_for_replication_jobs(Db);
        _ -> ok
    end,
    scan_for_replication_jobs(Rest);
scan_for_replication_jobs(Db) ->
    twig:log(notice, "Scanning ~p for replication jobs.", [Db]).

%% enhance for dbcore.
is_replicator_db(DbName) ->
    <<"_replicator">> =:= mem3:dbname(DbName).

%% SLOW
do_async(M, F, A) ->
    {Pid, Ref} = spawn_monitor(fun() ->
        exit(erlang:apply(M, F, A))
    end),
    receive
        {'DOWN', Ref, process, Pid, Result} ->
            Result
    end.
