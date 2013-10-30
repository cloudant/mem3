-module(mem3_rep).

-export([go/2, go/3, changes_enumerator/3, make_local_id/2]).
-export([save_checkpoint/5]).

-include("mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(CTX, #user_ctx{roles = [<<"_admin">>]}).

-record(acc, {
    batch_size,
    batch_count,
    revcount = 0,
    infos = [],
    seq,
    localid,
    source,
    target,
    filter,
    history
}).

go(Source, Target) ->
    go(Source, Target, []).

go(DbName, Node, Opts) when is_binary(DbName), is_atom(Node) ->
    go(#shard{name=DbName, node=node()}, #shard{name=DbName, node=Node}, Opts);

go(#shard{} = Source, #shard{} = Target, Opts) ->
    mem3_sync_security:maybe_sync(Source, Target),
    BatchSize = case proplists:get_value(batch_size, Opts) of
        BS when is_integer(BS), BS > 0 -> BS;
        _ -> 100
    end,
    BatchCount = case proplists:get_value(batch_count, Opts) of
        all -> all;
        BC when is_integer(BC), BC > 0 -> BC;
        _ -> 1
    end,
    Filter = proplists:get_value(filter, Opts),
    LocalId = make_local_id(Source, Target, Filter),
    Acc = #acc{
        batch_size = BatchSize,
        batch_count = BatchCount,
        localid = LocalId,
        source = Source,
        target = Target,
        filter = Filter
    },
    go(Acc).

go(#acc{source=Source, batch_count=BC}=Acc) ->
    case couch_db:open(Source#shard.name, [{user_ctx,?CTX}]) of
    {ok, Db} ->
        Resp = try
            repl(Db, Acc)
        catch error:{not_found, no_db_file} ->
            {error, missing_target}
        after
            couch_db:close(Db)
        end,
        case Resp of
            {ok, P} when P > 0, BC == all ->
                go(Acc);
            {ok, P} when P > 0, BC > 1 ->
                go(Acc#acc{batch_count=BC-1});
            Else ->
                Else
        end;
    {not_found, no_db_file} ->
        {error, missing_source}
    end.

repl(#db{name=DbName, seq_tree=Bt}=Db, #acc{localid=LocalId}=Acc0) ->
    erlang:put(io_priority, {internal_repl, DbName}),
    {Seq, History} = calculate_start_seq(Db, Acc0#acc.target, LocalId),
    Acc1 = Acc0#acc{source=Db, seq=Seq, history=History},
    Fun = fun ?MODULE:changes_enumerator/3,
    {ok, _, Acc2} = couch_btree:fold(Bt, Fun, Acc1, [{start_key, Seq + 1}]),
    {ok, #acc{seq = LastSeq}} = replicate_batch(Acc2),
    {ok, couch_db:count_changes_since(Db, LastSeq)}.

make_local_id(#shard{}=Source, #shard{}=Target) ->
    make_local_id(Source, Target, undefined).

make_local_id(#shard{node=SourceNode}, #shard{node=TargetNode}, Filter) ->
    S = couch_util:encodeBase64Url(couch_util:md5(term_to_binary(SourceNode))),
    T = couch_util:encodeBase64Url(couch_util:md5(term_to_binary(TargetNode))),
    F = case is_function(Filter) of
        true ->
            {new_uniq, Hash} = erlang:fun_info(Filter, new_uniq),
            B = couch_util:encodeBase64Url(Hash),
            <<"-", B/binary>>;
        false ->
            <<>>
    end,
    <<"_local/shard-sync-", S/binary, "-", T/binary, F/binary>>.

changes_enumerator(FDI, _, #acc{revcount=C, infos=Infos}=Acc0) ->
    #doc_info{
        high_seq=Seq,
        revs=Revs
    } = couch_doc:to_doc_info(FDI),
    {Count, NewInfos} = case filter_doc(Acc0#acc.filter, FDI) of
        keep -> {C + length(Revs), [FDI | Infos]};
        discard -> {C, Infos}
    end,
    Acc1 = Acc0#acc{
        seq=Seq,
        revcount=Count,
        infos=NewInfos
    },
    Go = if Count < Acc1#acc.batch_size -> ok; true -> stop end,
    {Go, Acc1}.

filter_doc(Filter, FullDocInfo) when is_function(Filter) ->
    try Filter(FullDocInfo) of
        discard -> discard;
        _ -> keep
    catch _:_ ->
        keep
    end;
filter_doc(_, _) ->
    keep.

replicate_batch(#acc{target = #shard{node=Node, name=Name}} = Acc) ->
    case find_missing_revs(Acc) of
    [] ->
        ok;
    Missing ->
        ok = save_on_target(Node, Name, open_docs(Acc, Missing))
    end,
    update_locals(Acc),
    {ok, Acc#acc{revcount=0, infos=[]}}.

find_missing_revs(Acc) ->
    #acc{target = #shard{node=Node, name=Name}, infos = Infos} = Acc,
    IdsRevs = lists:map(fun(FDI) ->
        #doc_info{id=Id, revs=RevInfos} = couch_doc:to_doc_info(FDI),
        {Id, [R || #rev_info{rev=R} <- RevInfos]}
    end, Infos),
    Options = [{io_priority, {internal_repl, Name}}, {user_ctx, ?CTX}],
    rexi_call(Node, {fabric_rpc, get_missing_revs, [Name, IdsRevs, Options]}).

open_docs(#acc{source=Source, infos=Infos}, Missing) ->
    lists:flatmap(fun({Id, Revs, _}) ->
        FDI = lists:keyfind(Id, #full_doc_info.id, Infos),
        open_doc_revs(Source, FDI, Revs)
    end, Missing).

save_on_target(Node, Name, Docs) ->
    Options = [replicated_changes, full_commit, {user_ctx, ?CTX},
        {io_priority, {internal_repl, Name}}],
    rexi_call(Node, {fabric_rpc, update_docs, [Name, Docs, Options]}),
    ok.

update_locals(Acc) ->
    #acc{seq=Seq, source=Db, target=Target, localid=Id, history=History} = Acc,
    #shard{name=Name, node=Node} = Target,
    NewEntry0 = [
        {<<"source_seq">>, Seq},
        {<<"source_node">>, atom_to_binary(node(), utf8)},
        {<<"target_node">>, atom_to_binary(Node, utf8)},
        {<<"timestamp">>, list_to_binary(iso8601_timestamp())}
    ],
    FinalBody = rexi_call(Node, {?MODULE, save_checkpoint, [
        Name,
        Id,
        Seq,
        NewEntry0,
        History
    ]}),
    {ok, _} = couch_db:update_doc(Db, #doc{id = Id, body = FinalBody}, []).

save_checkpoint(DbName, Id, SourceSeq, NewEntry0, History0) ->
    erlang:put(io_priority, {internal_repl, DbName}),
    case couch_db:open_int(DbName, [{user_ctx, ?CTX}]) of
        {ok, #db{update_seq = TargetSeq} = Db} ->
            NewEntry = [
                {<<"target_seq">>, TargetSeq},
                {<<"target_uuid">>, couch_db:get_uuid(Db)}
                | NewEntry0
            ],
            Body = {[
                {<<"seq">>, SourceSeq},
                {<<"history">>, add_checkpoint(NewEntry, History0)}
            ]},
            Doc = #doc{id = Id, body = Body},
            rexi:reply(try couch_db:update_doc(Db, Doc, []) of
                {ok, _} ->
                    {ok, Body};
                Else ->
                    {error, Else}
            catch
                Exception ->
                    Exception;
                error:Reason ->
                    {error, Reason}
            end);
        Error ->
            rexi:reply(Error)
    end.

rexi_call(Node, MFA) ->
    Mon = rexi_monitor:start([rexi_utils:server_pid(Node)]),
    Ref = rexi:cast(Node, self(), MFA, [sync]),
    try
        receive {Ref, {ok, Reply}} ->
            Reply;
        {Ref, Error} ->
            erlang:error(Error);
        {rexi_DOWN, Mon, _, Reason} ->
            erlang:error({rexi_DOWN, {Node, Reason}})
        after 600000 ->
            erlang:error(timeout)
        end
    after
        rexi_monitor:stop(Mon)
    end.

calculate_start_seq(Db, #shard{node=Node, name=Name}, LocalId) ->
    case couch_db:open_doc(Db, LocalId, []) of
    {ok, #doc{body = {SProps}}} ->
        Opts = [{user_ctx, ?CTX}, {io_priority, {internal_repl, Name}}],
        try rexi_call(Node, {fabric_rpc, open_doc, [Name, LocalId, Opts]}) of
        #doc{body = {TProps}} ->
            SourceSeq = couch_util:get_value(<<"seq">>, SProps, 0),
            TargetSeq = couch_util:get_value(<<"seq">>, TProps, 0),
            Seq = erlang:min(SourceSeq, TargetSeq),
            {Seq, couch_util:get_value(<<"history">>, SProps, [])}
        catch error:{not_found, _} ->
            0
        end;
    {not_found, _} ->
        0
    end.

open_doc_revs(Db, #full_doc_info{id=Id, rev_tree=RevTree}, Revs) ->
    {FoundRevs, _} = couch_key_tree:get_key_leafs(RevTree, Revs),
    lists:map(fun({#leaf{deleted=IsDel, ptr=SummaryPtr}, FoundRevPath}) ->
                  couch_db:make_doc(Db, Id, IsDel, SummaryPtr, FoundRevPath)
    end, FoundRevs).

iso8601_timestamp() ->
    {_,_,Micro} = Now = os:timestamp(),
    {{Year,Month,Date},{Hour,Minute,Second}} = calendar:now_to_datetime(Now),
    Format = "~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0B.~6.10.0BZ",
    io_lib:format(Format, [Year, Month, Date, Hour, Minute, Second, Micro]).

add_checkpoint(Props, History) ->
    %% Split checkpoints into buckets of exponentially increasing delta from
    %% current sequence, keep the newest and oldest checkpoint in each bucket.
    NewSeq = couch_util:get_value(<<"source_seq">>, Props),
    {_, High, Low, Result} = lists:foldl(fun({Entry}, Acc) ->
        {Shift, High, Low, Keep} = Acc,
        CurSeq = couch_util:get_value(<<"source_seq">>, Entry),
        if (NewSeq - CurSeq) >  (2 bsl Shift) ->
            {Shift+1, {Entry}, nil , [Low, High | Keep]};
        true ->
            {Shift, High, {Entry}, Keep}
        end
    end, {2, {Props}, nil, []}, History),
    lists:reverse(lists:filter(fun(E) -> E /= nil end, [Low, High | Result])).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

add_checkpoint_test() ->
    Out = add_checkpoint([{<<"source_seq">>, 1000}], [
        {[{<<"source_seq">>, 995}]},
        {[{<<"source_seq">>, 993}]},
        {[{<<"source_seq">>, 990}]},
        {[{<<"source_seq">>, 980}]},
        {[{<<"source_seq">>, 975}]},
        {[{<<"source_seq">>, 970}]}
    ]),
    Correct = [
        % 0 - 8 bucket
        {[{<<"source_seq">>, 1000}]},
        {[{<<"source_seq">>, 993}]},
        % 8 - 16 bucket (only one entry, test that nil is filtered out)
        {[{<<"source_seq">>, 990}]},
        % 16 - 32 bucket
        {[{<<"source_seq">>, 980}]},
        {[{<<"source_seq">>, 970}]}
    ],
    ?assertEqual(Correct, Out).

-endif.
