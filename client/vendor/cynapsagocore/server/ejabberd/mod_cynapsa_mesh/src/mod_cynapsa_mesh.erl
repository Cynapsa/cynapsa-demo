%%%-------------------------------------------------------------------
%%% Cynapsa mesh authority for ejabberd 26.04.
%%%
%%% This module deliberately supports one ejabberd node and the Mnesia
%%% mod_shared_roster backend only.  Authorization reads the authoritative
%%% SRG tables on every bind and routed agent stanza; it has no allow cache.
%%%-------------------------------------------------------------------
-module(mod_cynapsa_mesh).

-behaviour(gen_mod).
-behaviour(gen_server).

%% gen_mod
-export([start/2, stop/1, reload/3, depends/2,
         mod_options/1, mod_opt_type/1, mod_doc/0]).
%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%% hooks
-export([c2s_handle_bind/1, filter_packet/1, user_send_packet/1,
         user_receive_packet/1, c2s_copy_session/2,
         c2s_session_resumed/1,
         c2s_authenticated_packet/2, c2s_handle_send/3,
         offline_message_guard/1, process_group_iq/1, decode_iq_subel/1,
         disco_local_features/5]).
%% Exact local XEP-0363 HTTP gate.
-export([process/2]).
%% restricted administrative API
-export([cynapsa_mesh_ready/1, cynapsa_mesh_create/2,
         cynapsa_mesh_add/3, cynapsa_mesh_remove/3,
         cynapsa_mesh_remove_many/3, cynapsa_mesh_snapshot/2,
         get_commands_spec/0]).

-ifdef(TEST).
-export([test/0]).
-endif.

-include("logger.hrl").
-include("ejabberd_commands.hrl").
-include("ejabberd_http.hrl").
-include_lib("xmpp/include/xmpp.hrl").
-include("mod_mam.hrl").
-include("mod_shared_roster.hrl").

-define(GROUP_PREFIX, <<"cynapsa_mesh_">>).
-define(MAX_MESH_ID_BYTES, 256).
-define(MAX_USER_BYTES, 256).
-define(MAX_MEMBERS, 65536).
-define(MAX_MESHES, 65536).
-define(CALL_TIMEOUT, 5000).
-define(HOOK_PRIORITY, 40).
-define(ERROR_META, cynapsa_mesh_server_error).
-define(SESSION_META, cynapsa_mesh_session_epoch).
-define(MAILBOX_META, cynapsa_mesh_mailbox_key).
-define(CUSTODY_META, cynapsa_mesh_custody_accepted).
-define(SYNC_META, cynapsa_mesh_terminal_sync).
-define(RESUME_META, cynapsa_mesh_authority_resume).
-define(UPLOAD_META, cynapsa_mesh_upload_epoch).
-define(UPLOAD_INSTANCE_KEY, {?MODULE, upload_instance}).
-define(AUTHORITY_HOST_KEY, {?MODULE, authority_host}).
-define(MAILBOX_AUDIT_KEY(Host), {?MODULE, mailbox_audit, Host}).
-define(AUTHORITY_NAMESPACE, <<"urn:cynapsa:mesh-authority:1">>).
-define(CONTROL_ID_PREFIX, <<"cynapsa-membership-">>).
-define(DEFAULT_CONTROL_ACK_TIMEOUT_SECONDS, 5).
-define(MAX_CONTROL_ACK_TIMEOUT_SECONDS, 60).
-define(DEFAULT_CONTROL_PENDING, 65536).
-define(CONTROL_SWEEP_MILLISECONDS, 250).
-define(TIME_NAMESPACE, <<"urn:xmpp:time">>).
-define(DISCO_INFO_NAMESPACE, <<"http://jabber.org/protocol/disco#info">>).
-define(DISCO_ITEMS_NAMESPACE, <<"http://jabber.org/protocol/disco#items">>).
-define(EXTDISCO_NAMESPACE, <<"urn:xmpp:extdisco:2">>).
-define(PING_NAMESPACE, <<"urn:xmpp:ping">>).
-define(CARBONS_NAMESPACE, <<"urn:xmpp:carbons:2">>).
-define(UPLOAD_NAMESPACE, <<"urn:xmpp:http:upload:0">>).
-define(UPLOAD_PREFIX, <<"upload.">>).
-define(UPLOAD_ID_PREFIX, <<"cynapsa-upload-">>).
-define(UPLOAD_FILENAME_PREFIX, <<"cynapsa-">>).
-define(UPLOAD_FILENAME_SUFFIX, <<".bin">>).
-define(UPLOAD_RANDOM_BYTES, 26).
-define(MAX_UPLOAD_PENDING, 256).
-define(MAX_UPLOAD_ID_BYTES, 64).
-define(MAX_UPLOAD_FILENAME_BYTES, 256).
-define(MAX_UPLOAD_CONTENT_TYPE_BYTES, 256).
%% Core V1 permits 134,217,696 payload bytes plus the exact 64-byte
%% authenticated-transfer allowance.
-define(MAX_UPLOAD_BYTES, 134217760).
-define(MAX_UPLOAD_URL_BYTES, 16384).
-define(MAX_UPLOAD_HEADERS, 16).
-define(MAX_UPLOAD_HEADER_NAME_BYTES, 256).
-define(MAX_UPLOAD_HEADER_VALUE_BYTES, 4096).
-define(UPLOAD_PENDING_MILLISECONDS, 30000).
-define(UPLOAD_SLOT_MILLISECONDS, 18000000).
-define(UPLOAD_SWEEP_MILLISECONDS, 60000).
-define(MAX_RETAINED_UPLOAD_OBJECTS, 4096).
-define(MAX_RETAINED_UPLOAD_BYTES, 4294967296).
-define(MAX_OWNER_UPLOAD_OBJECTS, 256).
-define(MAX_OWNER_UPLOAD_BYTES, 1073741824).
-define(MAX_PAGE_MEMBERS, 256).
-define(MAX_PAGE_BYTES, 262144).
-define(MAX_PENDING_SYNCS, 1).
-define(MAX_PENDING_SNAPSHOT_MEMBERS, 65536).
-define(MAX_PENDING_SNAPSHOT_BYTES, 67108864).
-define(SNAPSHOT_PENDING_MILLISECONDS, 5000).
-define(SNAPSHOT_SWEEP_MILLISECONDS, 250).
-define(AZTM_NAMESPACE, <<"urn:cynapsa:aztm:1">>).
-define(CUSTODY_NAMESPACE, <<"urn:cynapsa:mesh-custody:1">>).
-define(DEFAULT_MAILBOX_MAX_MESSAGES, 1024).
-define(DEFAULT_MAILBOX_MAX_BYTES, 67108864).
-define(DEFAULT_MAILBOX_RETENTION_SECONDS, 86400).
-define(MAX_MAILBOX_RETENTION_SECONDS, 86400).
-define(MAILBOX_SWEEP_MILLISECONDS, 60000).

-record(cynapsa_mesh_upload_object,
        {key,
         owner,
         path,
         slot}).

%% Durable ownership is the exact logical destination resource.  Neither row
%% contains an XMPP SID, c2s PID, or XEP-0198 resume ID.
-record(cynapsa_mesh_mailbox,
        {key,
         owner,
         sender,
         message_id,
         packet,
         bytes,
         inserted_at,
         expires_at}).

-record(cynapsa_mesh_mailbox_cursor,
        {owner,
         next_sequence = 1,
         message_count = 0,
         byte_count = 0}).

-record(state, {host :: binary(),
                %% Exact live c2s pid -> {session epoch, user, mesh}.  An entry
                %% exists only after this exact session consumed one complete
                %% current membership snapshot.
                ready = #{} :: map(),
                monitors = #{} :: map(),
                %% Exact live c2s pid -> immutable snapshot paging ownership.
                %% Aggregate count and byte budgets prevent one frozen vector
                %% per live member from growing into O(N²) authority memory.
                %% Membership mutations clear these entries before notifying
                %% live sessions, so pages from different snapshots never mix.
                pending_sync = #{} :: map(),
                pending_sync_members = 0 :: non_neg_integer(),
                pending_sync_bytes = 0 :: non_neg_integer(),
                %% Exact c2s PID -> {SID, user, mesh}.  A terminal authority
                %% result has been produced but has not yet crossed the c2s
                %% send hook.  Mailbox replay cannot precede that result.
                terminal_sync = #{} :: map(),
                %% Replacement c2s PID -> server-generated handoff ticket and
                %% the authority status owned by the old logical stream.
                %% A ticket exists only between c2s_copy_session and the
                %% successful c2s_session_resumed hook. Membership mutation
                %% removes tickets for that mesh before notifications route.
                resume_transfers = #{} :: map(),
                %% Server-generated membership IQ id -> exact session owner and
                %% bounded monotonic deadline. The entry is removed only by a
                %% strict processed acknowledgement, session loss, or timeout.
                pending_controls = #{} :: map(),
                control_ack_timeout_ms =
                    ?DEFAULT_CONTROL_ACK_TIMEOUT_SECONDS * 1000
                    :: pos_integer(),
                control_pending_max = ?DEFAULT_CONTROL_PENDING
                    :: pos_integer(),
                mailbox_max_messages = ?DEFAULT_MAILBOX_MAX_MESSAGES
                    :: pos_integer(),
                mailbox_max_bytes = ?DEFAULT_MAILBOX_MAX_BYTES
                    :: pos_integer(),
                mailbox_retention_seconds = ?DEFAULT_MAILBOX_RETENTION_SECONDS
                    :: pos_integer(),
                %% Exact live c2s pid -> {SID, bound JID,
                %%                          #{bounded IQ id => monotonic expiry}}.
                %% URLs, headers, filenames, and credentials are never retained.
                upload_pending = #{} :: map()}).

%%%===================================================================
%%% gen_mod lifecycle
%%%===================================================================

start(Host, Opts) ->
    %% V1 has one authority vhost.  A second instance would duplicate the
    %% global pre-routing hook and make command ownership ambiguous.
    case gen_mod:is_loaded_elsewhere(Host, ?MODULE) of
        true ->
            {error, clustered_or_multi_host_authority_unsupported};
        false ->
            ejabberd_commands:register_commands(?MODULE, get_commands_spec()),
            case gen_mod:start_child(?MODULE, Host, Opts) of
                {ok, _Pid} = OK -> OK;
                Error ->
                    ejabberd_commands:unregister_commands(get_commands_spec()),
                    Error
            end
    end.

stop(Host) ->
    Result = gen_mod:stop_child(?MODULE, Host),
    ejabberd_commands:unregister_commands(get_commands_spec()),
    Result.

reload(Host, NewOpts, _OldOpts) ->
    persistent_term:put(?MAILBOX_AUDIT_KEY(Host),
                        maps:get(mailbox_audit_log, NewOpts, false)),
    gen_server:call(proc(Host), verify_authority, ?CALL_TIMEOUT).

depends(_Host, _Opts) ->
    [{mod_shared_roster, hard}, {mod_offline, hard}, {mod_mam, hard}].

mod_options(_Host) ->
    [{mailbox_max_messages, ?DEFAULT_MAILBOX_MAX_MESSAGES},
     {mailbox_max_bytes, ?DEFAULT_MAILBOX_MAX_BYTES},
     {mailbox_retention_seconds, ?DEFAULT_MAILBOX_RETENTION_SECONDS},
     {control_ack_timeout_seconds, ?DEFAULT_CONTROL_ACK_TIMEOUT_SECONDS},
     {control_pending_max, ?DEFAULT_CONTROL_PENDING},
     {mailbox_audit_log, false}].

mod_opt_type(Option) when Option =:= mailbox_max_messages;
                          Option =:= mailbox_max_bytes ->
    fun(Value) when is_integer(Value), Value > 0 -> Value end;
mod_opt_type(mailbox_retention_seconds) ->
    %% Core keeps terminal message IDs for 25 hours. Keep the durable replay
    %% horizon at or below 24 hours so receiver dedupe always has a safety
    %% margin across independent Core and authority clocks.
    fun(Value) when is_integer(Value), Value > 0,
                    Value =< ?MAX_MAILBOX_RETENTION_SECONDS -> Value end;
mod_opt_type(control_ack_timeout_seconds) ->
    fun(Value) when is_integer(Value), Value > 0,
                    Value =< ?MAX_CONTROL_ACK_TIMEOUT_SECONDS -> Value end;
mod_opt_type(control_pending_max) ->
    fun(Value) when is_integer(Value), Value > 0,
                    Value =< ?MAX_MEMBERS -> Value end;
mod_opt_type(mailbox_audit_log) ->
    fun(Value) when is_boolean(Value) -> Value end;
mod_opt_type(_Option) -> fun(Value) -> Value end.

mod_doc() ->
    #{desc => <<"Cynapsa single-node mesh membership and routing authority">>}.

%%%===================================================================
%%% gen_server lifecycle and serialized mutations
%%%===================================================================

init([Host, Opts]) ->
    process_flag(trap_exit, true),
    %% This module owns the authority capability for every bound resource on
    %% the dedicated host.  A module restart has lost its exact-session state;
    %% close those sessions before rebuilding state so retained Core authority
    %% cannot outlive the server authority owner.
    Fence = fence_existing_host_sessions(Host),
    case {Fence, initialize_authority(Host)} of
        {ok, ok} ->
            persistent_term:put(?AUTHORITY_HOST_KEY, Host),
            persistent_term:put(?UPLOAD_INSTANCE_KEY,
                                crypto:strong_rand_bytes(16)),
            persistent_term:put(?MAILBOX_AUDIT_KEY(Host),
                                maps:get(mailbox_audit_log, Opts, false)),
            ejabberd_hooks:add(c2s_handle_bind, Host, ?MODULE,
                               c2s_handle_bind, ?HOOK_PRIORITY),
            ejabberd_hooks:add(filter_packet, ?MODULE,
                               filter_packet, ?HOOK_PRIORITY),
            ejabberd_hooks:add(user_send_packet, Host, ?MODULE,
                               user_send_packet, ?HOOK_PRIORITY),
            ejabberd_hooks:add(user_receive_packet, Host, ?MODULE,
                               user_receive_packet, ?HOOK_PRIORITY),
            ejabberd_hooks:add(c2s_copy_session, Host, ?MODULE,
                               c2s_copy_session, ?HOOK_PRIORITY),
            ejabberd_hooks:add(c2s_session_resumed, Host, ?MODULE,
                               c2s_session_resumed, ?HOOK_PRIORITY),
            ejabberd_hooks:add(c2s_authenticated_packet, Host, ?MODULE,
                               c2s_authenticated_packet, ?HOOK_PRIORITY),
            ejabberd_hooks:add(c2s_handle_send, Host, ?MODULE,
                               c2s_handle_send, 60),
            ejabberd_hooks:add(offline_message_hook, Host, ?MODULE,
                               offline_message_guard, ?HOOK_PRIORITY),
            ejabberd_hooks:add(disco_local_features, Host, ?MODULE,
                               disco_local_features, ?HOOK_PRIORITY),
            gen_iq_handler:add_iq_handler(ejabberd_local, Host,
                                           ?AUTHORITY_NAMESPACE, ?MODULE,
                                           process_group_iq),
            _ = erlang:send_after(?SNAPSHOT_SWEEP_MILLISECONDS, self(),
                                  snapshot_sweep),
            _ = erlang:send_after(?UPLOAD_SWEEP_MILLISECONDS, self(),
                                  upload_sweep),
            _ = erlang:send_after(?MAILBOX_SWEEP_MILLISECONDS, self(),
                                  mailbox_sweep),
            _ = erlang:send_after(?CONTROL_SWEEP_MILLISECONDS, self(),
                                  control_sweep),
            {ok, #state{
                    host = Host,
                    control_ack_timeout_ms = maps:get(
                                               control_ack_timeout_seconds,
                                               Opts,
                                               ?DEFAULT_CONTROL_ACK_TIMEOUT_SECONDS)
                                             * 1000,
                    control_pending_max = maps:get(
                                            control_pending_max, Opts,
                                            ?DEFAULT_CONTROL_PENDING),
                    mailbox_max_messages = maps:get(
                                             mailbox_max_messages, Opts,
                                             ?DEFAULT_MAILBOX_MAX_MESSAGES),
                    mailbox_max_bytes = maps:get(
                                          mailbox_max_bytes, Opts,
                                          ?DEFAULT_MAILBOX_MAX_BYTES),
                    mailbox_retention_seconds = maps:get(
                                                  mailbox_retention_seconds,
                                                  Opts,
                                                  ?DEFAULT_MAILBOX_RETENTION_SECONDS)}};
        {{error, Reason}, _} ->
            {stop, Reason};
        {_, {error, Reason}} ->
            {stop, Reason}
    end.

handle_call(verify_authority, _From, #state{host = Host} = State) ->
    {reply, initialize_authority(Host), State};
handle_call({create, MeshID}, _From, #state{host = Host} = State) ->
    Reply = create_mesh(Host, MeshID),
    {reply, Reply, State};
handle_call({add, User, MeshID}, _From, #state{host = Host} = State) ->
    mutation_reply(add_member(Host, User, MeshID), [], State);
handle_call({remove, User, MeshID}, _From, #state{host = Host} = State) ->
    mutation_reply(remove_member(Host, [User], MeshID), [User], State);
handle_call({remove_many, Users, MeshID}, _From, #state{host = Host} = State) ->
    mutation_reply(remove_member(Host, Users, MeshID), Users, State);
handle_call({snapshot, MeshID}, _From, #state{host = Host} = State) ->
    {reply, read_snapshot(Host, MeshID), State};
handle_call({authority_sync, From, Origin, Nonce, Cursor}, _From,
            #state{host = Host, ready = Ready,
                   monitors = Monitors, pending_sync = Pending,
                   pending_sync_members = PendingMembers,
                   pending_sync_bytes = PendingBytes,
                   upload_pending = UploadPending} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case authority_sync_owned(Host, From, Origin, Nonce, Cursor, Pending,
                              PendingMembers, PendingBytes, Now) of
        {ok, {Pid, SID}, User, MeshID, Members, Results, UpdatedPending0} ->
            PendingState = update_pending_state(Pid, UpdatedPending0, State),
            UpdatedMonitors = case maps:is_key(Pid, Monitors) of
                                  true -> Monitors;
                                  false -> Monitors#{Pid => erlang:monitor(process, Pid)}
                              end,
            Next = Cursor + length(Results), Total = length(Members),
            case Next of
                Total ->
                    UpdatedUploadPending = transition_upload_pending(
                                             Pid, SID, From, User, MeshID,
                                             Ready,
                                             UploadPending, Now),
                    Terminal = State#state.terminal_sync,
                    {reply, {ok, terminal, Total, Cursor, Results,
                             Pid, SID, User, MeshID},
                     PendingState#state{ready = Ready,
                                        terminal_sync = Terminal#{
                                          Pid => {SID, User, MeshID}},
                                        upload_pending = UpdatedUploadPending,
                                        monitors = UpdatedMonitors}};
                _ when Next < Total ->
                    {reply, {ok, Total, Cursor, Results},
                     PendingState#state{monitors = UpdatedMonitors}};
                _ ->
                    {reply, {error, snapshot_conflict},
                     remove_pending_pid(Pid, State)}
            end;
        {error, snapshot_expired, Pid, User, MeshID} ->
            close_expired_snapshot(Pid, Host, User, MeshID),
            {reply, {error, snapshot_expired}, remove_pending_pid(Pid, State)};
        Error -> {reply, Error, State}
    end;
handle_call({route_authorized, Packet, From, To}, _From,
            #state{host = Host, ready = Ready} = State) ->
    {reply, route_ready(Host, Packet, From, To, Ready), State};
handle_call({prepare_resume, OldPid, OldSID, NewPid, User, MeshID}, _From,
            #state{host = Host, ready = Ready, monitors = Monitors,
                   resume_transfers = Transfers,
                   pending_controls = Pending} = State) ->
    case member_authorized(Host, MeshID, {User, Host}) of
        true ->
            BoundJID = jid:make(User, Host, MeshID),
            %% Reserve an exact unacknowledged control on the replacement PID
            %% even when that control already invalidated application
            %% readiness. The old SID remains until complete_resume installs
            %% NewSID, so neither process can acknowledge during the handoff.
            UpdatedControls = transfer_control_owner(
                                Pending, OldPid, OldSID, NewPid, OldSID,
                                BoundJID),
            case prepare_resume_transfer(
                   OldPid, OldSID, NewPid, User, Host, MeshID, Ready,
                   fun current_session_pid/3) of
                {ok, Token} ->
                    UpdatedMonitors = ensure_session_monitor(NewPid, Monitors),
                    Transfer = {Token, OldPid, OldSID, User, MeshID, ready},
                    {reply, {ok, Token},
                     State#state{
                       ready = maps:remove(OldPid, Ready),
                       resume_transfers = Transfers#{NewPid => Transfer},
                       pending_controls = UpdatedControls,
                       monitors = UpdatedMonitors}};
                not_ready when UpdatedControls =/= Pending ->
                    Token = make_ref(),
                    UpdatedMonitors = ensure_session_monitor(NewPid, Monitors),
                    Transfer = {Token, OldPid, OldSID, User, MeshID,
                                not_ready},
                    {reply, {ok, Token},
                     State#state{
                       resume_transfers = Transfers#{NewPid => Transfer},
                       pending_controls = UpdatedControls,
                       monitors = UpdatedMonitors}};
                _ ->
                    {reply, not_ready, State}
            end;
        false ->
            {reply, not_ready, State}
    end;
handle_call({complete_resume, NewPid, NewSID, User, MeshID, Token}, _From,
            #state{host = Host, ready = Ready,
                   resume_transfers = Transfers,
                   pending_controls = Pending} = State) ->
    Completion = case member_authorized(Host, MeshID, {User, Host}) of
                     true -> complete_resume_transfer(
                               NewPid, NewSID, User, Host, MeshID, Token,
                               Transfers, fun current_session_pid/3);
                     false -> error
                 end,
    case Completion of
        {ok, Status} ->
            {Token, _OldPid, OldSID, User, MeshID, Status} =
                maps:get(NewPid, Transfers),
            BoundJID = jid:make(User, Host, MeshID),
            UpdatedControls = transfer_control_owner(
                                Pending, NewPid, OldSID, NewPid, NewSID,
                                BoundJID),
            UpdatedReady = case Status of
                               ready -> Ready#{NewPid =>
                                                   {NewSID, User, MeshID}};
                               not_ready -> Ready
                           end,
            {reply, Status,
             State#state{ready = UpdatedReady,
                         resume_transfers = maps:remove(NewPid, Transfers),
                         pending_controls = UpdatedControls}};
        _ ->
            {reply, not_ready,
             State#state{resume_transfers = maps:remove(NewPid, Transfers)}}
    end;
handle_call({control_ack, Pid, SID, BoundJID, ID}, _From,
            #state{pending_controls = Pending} = State) ->
    case consume_control_ack(Pid, SID, BoundJID, ID, Pending) of
        {ok, Updated} ->
            {reply, ok, State#state{pending_controls = Updated}};
        error ->
            close_exact_session(Pid, authority_control_invalid),
            {reply, {error, invalid_control_ack},
             State#state{pending_controls =
                           remove_control_owner(Pid, Pending)}}
    end;
handle_call({invalid_control_ack, Pid}, _From,
            #state{pending_controls = Pending} = State) ->
    close_exact_session(Pid, authority_control_invalid),
    {reply, {error, invalid_control_ack},
     State#state{pending_controls = remove_control_owner(Pid, Pending)}};
handle_call({mailbox_route, Packet, From, To}, _From,
            #state{host = Host, ready = Ready,
                   mailbox_max_messages = MaxMessages,
                   mailbox_max_bytes = MaxBytes,
                   mailbox_retention_seconds = Retention} = State) ->
    case route_sender_ready(Host, Packet, From, To, Ready) of
        true ->
            case store_mailbox_packet(Packet, Host, MaxMessages, MaxBytes,
                                      Retention) of
                {ok, Key, StoredPacket} ->
                    Dispatch = dispatch_mailbox_packet(Key, StoredPacket, To,
                                                       Ready),
                    Custody = dispatch_custody_accepted(Packet, From, Host,
                                                        Ready),
                    mailbox_audit_admission(Host, Packet, Key, Dispatch,
                                            Custody),
                    {reply, stored, State};
                {error, full} -> {reply, {error, mailbox_full}, State};
                {error, _} -> {reply, {error, mailbox_unavailable}, State}
            end;
        false -> {reply, {error, unauthorized}, State}
    end;
handle_call({delivery_authorized, Packet, RecipientEpoch}, _From,
            #state{host = Host, ready = Ready} = State) ->
    {reply, delivery_ready(Host, Packet, RecipientEpoch, Ready),
     State};
handle_call({upload_request, Packet}, _From,
            #state{host = Host, ready = Ready,
                   upload_pending = Pending} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case register_upload_request(Host, Packet, Ready, Pending, Now) of
        {ok, Meta, Updated} ->
            {reply, {ok, Meta}, State#state{upload_pending = Updated}};
        error -> {reply, {error, unauthorized}, State}
    end;
handle_call({upload_response, Packet}, _From,
            #state{host = Host, ready = Ready,
                   upload_pending = Pending} = State) ->
    Now = erlang:monotonic_time(millisecond),
    case consume_upload_response(Host, Packet, Ready, Pending, Now) of
        {ok, Meta, Updated} ->
            {reply, {ok, Meta}, State#state{upload_pending = Updated}};
        error ->
            _ = cancel_upload_response(Packet, Host),
            {reply, {error, unauthorized}, State}
    end;
handle_call({upload_delivery, Packet, RecipientEpoch}, _From,
            #state{host = Host, ready = Ready} = State) ->
    {reply, upload_delivery_current(Host, Packet, RecipientEpoch, Ready),
     State};
handle_call(_Request, _From, State) ->
    {reply, {error, unsupported_request}, State}.

handle_cast({complete_sync, Pid, SID, User, MeshID},
            #state{host = Host, terminal_sync = Terminal,
                   ready = Ready} = State) ->
    case maps:get(Pid, Terminal, none) of
        {SID, User, MeshID} ->
            case exact_session_epoch_current(Pid, SID, User, Host, MeshID,
                                              #{Pid => {SID, User, MeshID}},
                                              fun current_session_pid/3)
                 andalso member_authorized(Host, MeshID, {User, Host}) of
                true ->
                    UpdatedReady = Ready#{Pid => {SID, User, MeshID}},
                    case drain_mailbox(Host, User, MeshID, Pid,
                                       UpdatedReady) of
                        ok ->
                            {noreply, State#state{
                                        ready = UpdatedReady,
                                        terminal_sync = maps:remove(Pid,
                                                                    Terminal)}};
                        {error, _} ->
                            _ = catch ejabberd_c2s:close(Pid,
                                                        mailbox_unavailable),
                            {noreply, State#state{
                                        terminal_sync = maps:remove(Pid,
                                                                    Terminal)}}
                    end;
                false ->
                    {noreply, State#state{
                                terminal_sync = maps:remove(Pid, Terminal)}}
            end;
        _ -> {noreply, State}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason},
            #state{ready = Ready, monitors = Monitors,
                   terminal_sync = Terminal,
                   resume_transfers = Transfers,
                   pending_controls = Controls,
                   upload_pending = UploadPending} = State) ->
    case maps:get(Pid, Monitors, undefined) of
        Ref ->
            PendingState = remove_pending_pid(Pid, State),
            {noreply, PendingState#state{
                        ready = maps:remove(Pid, Ready),
                        terminal_sync = maps:remove(Pid, Terminal),
                        resume_transfers = maps:remove(Pid, Transfers),
                        pending_controls = remove_control_owner(Pid, Controls),
                        monitors = maps:remove(Pid, Monitors),
                        upload_pending = maps:remove(Pid, UploadPending)}};
        _ -> {noreply, State}
    end;
handle_info(snapshot_sweep, State) ->
    Now = erlang:monotonic_time(millisecond),
    Updated = expire_pending_snapshots(Now, State),
    _ = erlang:send_after(?SNAPSHOT_SWEEP_MILLISECONDS, self(),
                          snapshot_sweep),
    {noreply, Updated};
handle_info(upload_sweep, #state{host = Host} = State) ->
    _ = prune_expired_uploads(Host),
    _ = erlang:send_after(?UPLOAD_SWEEP_MILLISECONDS, self(), upload_sweep),
    {noreply, State};
handle_info(mailbox_sweep, #state{host = Host} = State) ->
    _ = prune_expired_mailbox(Host),
    _ = erlang:send_after(?MAILBOX_SWEEP_MILLISECONDS, self(), mailbox_sweep),
    {noreply, State};
handle_info(control_sweep, State) ->
    Now = erlang:monotonic_time(millisecond),
    Updated = expire_control_acks(Now, State),
    _ = erlang:send_after(?CONTROL_SWEEP_MILLISECONDS, self(), control_sweep),
    {noreply, Updated};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{host = Host}) ->
    ejabberd_hooks:delete(c2s_handle_bind, Host, ?MODULE,
                          c2s_handle_bind, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(filter_packet, ?MODULE,
                          filter_packet, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE,
                          user_send_packet, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(user_receive_packet, Host, ?MODULE,
                          user_receive_packet, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(c2s_copy_session, Host, ?MODULE,
                          c2s_copy_session, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(c2s_session_resumed, Host, ?MODULE,
                          c2s_session_resumed, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(c2s_authenticated_packet, Host, ?MODULE,
                          c2s_authenticated_packet, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(c2s_handle_send, Host, ?MODULE,
                          c2s_handle_send, 60),
    ejabberd_hooks:delete(offline_message_hook, Host, ?MODULE,
                          offline_message_guard, ?HOOK_PRIORITY),
    ejabberd_hooks:delete(disco_local_features, Host, ?MODULE,
                          disco_local_features, ?HOOK_PRIORITY),
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host,
                                     ?AUTHORITY_NAMESPACE),
    persistent_term:erase(?AUTHORITY_HOST_KEY),
    persistent_term:erase(?UPLOAD_INSTANCE_KEY),
    persistent_term:erase(?MAILBOX_AUDIT_KEY(Host)),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Bind and routing authorization
%%%===================================================================

%% Advertise the one versioned Cynapsa authority capability only on the
%% authenticated local server's root disco#info node. Remove any preceding
%% duplicate before adding ours so this hook contributes exactly one feature.
disco_local_features({error, _} = Acc, _From, _To, _Node, _Lang) ->
    Acc;
disco_local_features(Acc, _From, _To, <<>>, _Lang) ->
    Features = case Acc of
                   {result, Existing} -> Existing;
                   empty -> []
               end,
    {result, [?AUTHORITY_NAMESPACE |
              [Feature || Feature <- Features,
                          Feature =/= ?AUTHORITY_NAMESPACE]]};
disco_local_features(Acc, _From, _To, _Node, _Lang) ->
    Acc.

%% Runs after SASL has authenticated the bare JID and before ejabberd opens
%% the resource session.  The requested resource is the exact mesh_id.
c2s_handle_bind({Resource, {ok, #{user := User,
                                  lserver := Host,
                                  lang := Lang} = C2SState}} = Acc) ->
    case bind_authorized(User, Host, Resource) of
        true -> Acc;
        false ->
            Text = <<"Cynapsa mesh membership does not authorize this resource">>,
            Error = xmpp:err_not_allowed(Text, Lang),
            {Resource, {error, Error, C2SState}}
    end;
c2s_handle_bind(Acc) ->
    Acc.

%% The global router invokes this before local IQ dispatch and before bare or
%% full JID session routing. Local agent message, IQ, and presence traffic is
%% governed. Server/service traffic remains available for XEP-0202 and other
%% authenticated service IQs.
filter_packet(#message{} = Packet) ->
    filter_agent_packet(Packet);
filter_packet(#iq{} = Packet) ->
    filter_agent_packet(Packet);
filter_packet(#presence{} = Packet) ->
    filter_agent_packet(Packet);
filter_packet(Packet) ->
    Packet.

filter_agent_packet(Packet) ->
    case {xmpp:get_meta(Packet, ?MAILBOX_META, undefined),
          xmpp:get_meta(Packet, ?CUSTODY_META, undefined)} of
        {undefined, undefined} -> filter_nonmailbox_packet(Packet);
        _ -> drop
    end.

filter_nonmailbox_packet(Packet) ->
    case xmpp:get_meta(Packet, ?ERROR_META, undefined) of
        undefined ->
            From = xmpp:get_from(Packet),
            To = xmpp:get_to(Packet),
            AuthorityHost = persistent_term:get(?AUTHORITY_HOST_KEY, undefined),
            case route_scope(From, To, AuthorityHost) of
                service ->
                    case approved_service_packet(Packet, From, To,
                                                 AuthorityHost) of
                        true -> Packet;
                        false -> reject_packet(Packet)
                    end;
                upload_service ->
                    case authorize_upload_route(Packet, From, To,
                                                AuthorityHost) of
                        {ok, Meta} -> xmpp:put_meta(Packet, ?UPLOAD_META, Meta);
                        error -> reject_packet(Packet)
                    end;
                {agent, Host, _Resource, _FromUS, _ToUS} ->
                    case {client_payload_safe(Packet),
                          authority_ready(Host),
                          cynapsa_message_kind(Packet)} of
                        {true, true, eligible} ->
                            case safe_call(Host, {mailbox_route, Packet,
                                                 From, To}) of
                                stored -> drop;
                                {error, mailbox_full} ->
                                    reject_mailbox_packet(Packet,
                                                          mailbox_full);
                                {error, mailbox_unavailable} ->
                                    reject_mailbox_packet(
                                      Packet, mailbox_unavailable);
                                _ -> reject_packet(Packet)
                            end;
                        {true, true, malformed} ->
                            reject_packet(Packet);
                        {true, true, transient} ->
                            case safe_call(Host,
                                           {route_authorized, Packet,
                                            From, To}) of
                                true -> strip_transient_transport_id(Packet);
                                _ -> reject_packet(Packet)
                            end;
                        {true, true, ordinary} ->
                            case safe_call(Host, {route_authorized, Packet,
                                                 From, To}) of
                                true -> Packet;
                                _ -> reject_packet(Packet)
                            end;
                        _ -> reject_packet(Packet)
                    end;
                deny -> reject_packet(Packet)
            end;
        _ ->
            %% ERROR_META is an internal routing capability, not a boolean
            %% exemption.  Only the exact sanitized error created for the
            %% still-current authenticated c2s session may use it.
            AuthorityHost = persistent_term:get(?AUTHORITY_HOST_KEY, undefined),
            case safe_policy_error(Packet, AuthorityHost) of
                true -> Packet;
                false -> drop
            end
    end.

%% Capture the exact c2s process and stream-management session epoch before
%% routing. A later full-JID replacement can never inherit this metadata.
user_send_packet({Packet, #{jid := JID, sid := SID} = C2SState})
  when is_pid(self()) ->
    AuthorityHost = persistent_term:get(?AUTHORITY_HOST_KEY, undefined),
    OriginPacket = xmpp:put_meta(Packet, ?SESSION_META, {self(), SID, JID}),
    case authority_control_ack_shape(OriginPacket, JID, AuthorityHost) of
        {ack, ID} ->
            _ = safe_call(AuthorityHost,
                          {control_ack, self(), SID, JID, ID}),
            {drop, C2SState};
        invalid ->
            _ = safe_call(AuthorityHost, {invalid_control_ack, self()}),
            {drop, C2SState};
        none ->
            case client_packet_allowed(OriginPacket, JID, AuthorityHost) of
                true ->
                    {OriginPacket, C2SState};
                false ->
                    _ = reject_packet(OriginPacket),
                    {drop, C2SState}
            end
    end;
user_send_packet(Acc) -> Acc.

%% The router check is not the final recipient boundary: the full resource may
%% be replaced before delivery. Recheck the exact destination c2s epoch here.
user_receive_packet({Packet, #{jid := JID, sid := SID} = C2SState} = Acc) ->
    case custody_receipt_for_session(Packet, {self(), SID, JID},
                                     JID#jid.lserver) of
        true -> Acc;
        false -> {drop, C2SState};
        none -> user_receive_non_custody(Packet, JID, SID, C2SState, Acc)
    end;
user_receive_packet(Acc) -> Acc.

user_receive_non_custody(Packet, JID, SID, C2SState, Acc) ->
    case xmpp:get_meta(Packet, ?ERROR_META, undefined) of
        undefined ->
            From = xmpp:get_from(Packet),
            To = xmpp:get_to(Packet),
            case route_scope(From, To, JID#jid.lserver) of
                {agent, Host, _Resource, _FromUS, _ToUS} ->
                    case client_payload_safe(Packet)
                        andalso safe_call(Host,
                                          {delivery_authorized, Packet,
                                           {self(), SID, JID}}) =:= true of
                        true -> Acc;
                        _ -> {drop, C2SState}
                    end;
                service ->
                    case approved_service_packet(Packet, From, To,
                                                 JID#jid.lserver) of
                        true -> Acc;
                        false -> {drop, C2SState}
                    end;
                upload_service ->
                    case safe_call(JID#jid.lserver,
                                   {upload_delivery, Packet,
                                    {self(), SID, JID}}) of
                        true -> Acc;
                        false -> {drop, C2SState}
                    end;
                deny -> {drop, C2SState}
            end;
        _ ->
            case safe_policy_error_for_session(
                   Packet, {self(), SID, JID}, JID#jid.lserver) of
                true -> Acc;
                false -> {drop, C2SState}
            end
    end.

%% The stock mod_http_upload process consumes a temporary slot before its
%% separate HTTP worker writes the file.  Serializing the entire HTTP
%% operation with removal closes the use_slot/file-creation race: a removal
%% which commits first makes this membership check fail, while a PUT which
%% acquires the path lock first must finish before removal deletes the object.
process(Path, #request{method = Method} = Request)
  when Method =:= 'PUT'; Method =:= 'GET'; Method =:= 'HEAD' ->
    case upload_http_path(Path) of
        {ok, Host, ObjectPath, Slot} ->
            case with_upload_path_lock(
                   ObjectPath,
                   fun() ->
                           case upload_http_authorized(Host, ObjectPath,
                                                       Slot, Method) of
                               true -> upload_http_delegate(
                                         Path, Request, Host, ObjectPath,
                                         Slot, Method);
                               false -> upload_http_forbidden()
                           end
                   end) of
                {ok, Response} -> Response;
                {error, _} -> upload_http_unavailable()
            end;
        error -> upload_http_forbidden()
    end;
process(Path, Request) ->
    mod_http_upload:process(Path, Request).

upload_http_path([UserDir, RandDir, Filename] = Slot) ->
    case persistent_term:get(?AUTHORITY_HOST_KEY, undefined) of
        Host when is_binary(Host) ->
            DocRoot0 = mod_http_upload_opt:docroot(Host),
            DocRoot1 = mod_http_upload:expand_home(DocRoot0),
            DocRoot = mod_http_upload:expand_host(DocRoot1, Host),
            Path = str:join([DocRoot, UserDir, RandDir, Filename], <<$/>>),
            case safe_upload_object_path(Host, Path, Slot) of
                true -> {ok, Host, Path, Slot};
                false -> error
            end;
        _ -> error
    end;
upload_http_path(_) -> error.

upload_http_authorized(Host, Path, Slot, Method) ->
    Key = upload_object_key(Host, Path),
    case catch mnesia:dirty_read(cynapsa_mesh_upload_object, Key) of
        [#cynapsa_mesh_upload_object{
            key = Key, owner = Owner, path = Path,
            slot = Slot}] ->
            upload_owner_http_allowed(Owner, Host, Method)
                andalso authority_ready(Host)
                andalso member_authorized(
                          Host, upload_owner_mesh(Owner),
                          {upload_owner_user(Owner), Host});
        _ -> false
    end.

upload_http_delegate(Path, Request, Host, ObjectPath, Slot, 'PUT') ->
    Response = mod_http_upload:process(Path, Request),
    case upload_http_success(Response) andalso
         complete_upload_object(Host, ObjectPath, Slot) =:= ok of
        true -> Response;
        false ->
            _ = retain_failed_upload_cleanup(Host, ObjectPath, Slot),
            upload_http_unavailable()
    end;
upload_http_delegate(Path, Request, _Host, _ObjectPath, _Slot, _Method) ->
    mod_http_upload:process(Path, Request).

upload_http_success({Status, _Headers, _Body}) ->
    Status >= 200 andalso Status < 300;
upload_http_success(_) -> false.

complete_upload_object(Host, Path, Slot) ->
    Key = upload_object_key(Host, Path),
    Token = persistent_term:get(?UPLOAD_INSTANCE_KEY, undefined),
    Now = erlang:monotonic_time(millisecond),
    Complete = fun() ->
                       case mnesia:read(cynapsa_mesh_upload_object, Key,
                                        write) of
                           [#cynapsa_mesh_upload_object{
                               owner = {User, Host, MeshID, pending, Size,
                                        Token, Expiry}, path = Path,
                               slot = Slot} = Object]
                             when is_integer(Expiry), Expiry > Now ->
                               mnesia:write(
                                 Object#cynapsa_mesh_upload_object{
                                   owner = {User, Host, MeshID, completed,
                                            Size}}),
                               ok;
                           _ -> mnesia:abort(upload_ownership_changed)
                       end
               end,
    case mnesia:transaction(Complete, [], 3) of
        {atomic, ok} -> ok;
        _ -> error
    end.

retain_failed_upload_cleanup(Host, Path, Slot) ->
    Key = upload_object_key(Host, Path),
    Tombstone = fun() ->
                        case mnesia:read(cynapsa_mesh_upload_object, Key,
                                         write) of
                            [#cynapsa_mesh_upload_object{
                                owner = {User, Host, MeshID, pending, Size,
                                         _Token, _Expiry}, path = Path,
                                slot = Slot} = Object] ->
                                Updated =
                                    Object#cynapsa_mesh_upload_object{
                                      owner = {User, Host, MeshID,
                                               cleanup_pending, Size}},
                                mnesia:write(Updated),
                                {ok, Updated};
                            [#cynapsa_mesh_upload_object{
                                owner = {_User, Host, _MeshID,
                                         cleanup_pending, _Size},
                                path = Path, slot = Slot} = Object] ->
                                {ok, Object};
                            _ -> mnesia:abort(upload_ownership_changed)
                        end
                end,
    case mnesia:transaction(Tombstone, [], 3) of
        {atomic, {ok, Object}} ->
            finish_failed_upload_cleanup(
              Object,
              fun() -> cleanup_upload_object_locked(Host, Path, Slot) end,
              fun delete_upload_record/1);
        _ -> error
    end.

finish_failed_upload_cleanup(Object, Cleanup, Delete)
  when is_function(Cleanup, 0), is_function(Delete, 1) ->
    case Cleanup() of
        ok -> Delete(Object);
        Error -> Error
    end.

upload_http_forbidden() -> {403, [], <<"Forbidden">>}.
upload_http_unavailable() -> {503, [], <<"Service Unavailable">>}.

with_upload_path_lock(Path, Fun) when is_binary(Path), is_function(Fun, 0) ->
    Lock = {{?MODULE, upload_path, Path}, self()},
    try global:trans(Lock, Fun, [node()]) of
        aborted -> {error, upload_path_lock_failed};
        Result -> {ok, Result}
    catch _:_ -> {error, upload_path_lock_failed}
    end.

%% Observe destination stream acknowledgements before mod_stream_mgmt drops
%% the cumulative queue prefix.  Mailbox records remain durable until this
%% exact acknowledgement; failure to delete is safe and yields a duplicate on
%% a later fresh bind rather than message loss.
c2s_authenticated_packet(#{lserver := Host,
                           jid := JID,
                           sid := SID,
                           mgmt_stanzas_out := Sent,
                           mgmt_queue := Queue} = State,
                         #sm_a{h = Handled})
  when is_integer(Handled), is_integer(Sent),
       Handled >= 0, Handled =< 16#ffffffff,
       Sent >= 0, Sent =< 16#ffffffff ->
    case sm_handled_not_ahead(Handled, Sent) of
        true ->
            Keys = acknowledged_mailbox_keys(Queue, Handled),
            Result = acknowledge_mailbox_keys(Host, Keys),
            mailbox_audit_ack(Host, JID, SID, sm, Handled, Keys, Result);
        false -> ok
    end,
    State;
c2s_authenticated_packet(State, _Packet) -> State.

%% XEP-0198 counters are uint32 serial numbers. The stream-management queue is
%% bounded far below half the sequence space, so a modular distance in the
%% lower half means Handled is at or behind Sent, including across wrap.
sm_handled_not_ahead(Handled, Sent) ->
    ((Sent - Handled) band 16#ffffffff) < 16#80000000.

%% The terminal authority result must cross the actual c2s send boundary
%% before replay starts.  The gen_server then serializes the FIFO drain with
%% every new mailbox admission and all membership mutations.
c2s_handle_send(#{lserver := Host, mgmt_state := MgmtState} = State,
                Packet, ok)
  when (MgmtState =:= active orelse MgmtState =:= pending orelse
        MgmtState =:= resumed),
       (is_record(Packet, message) orelse is_record(Packet, iq) orelse
        is_record(Packet, presence)) ->
    case xmpp:get_meta(Packet, ?SYNC_META, undefined) of
        {Pid, SID, User, MeshID} when Pid =:= self() ->
            gen_server:cast(proc(Host),
                            {complete_sync, Pid, SID, User, MeshID}),
            State;
        _ -> State
    end;
c2s_handle_send(#{lserver := Host} = State, Packet, ok)
  when is_record(Packet, message); is_record(Packet, iq);
       is_record(Packet, presence) ->
    case {xmpp:get_meta(Packet, ?SYNC_META, undefined),
          xmpp:get_meta(Packet, ?MAILBOX_META, undefined)} of
        {{Pid, SID, User, MeshID}, _} when Pid =:= self() ->
            gen_server:cast(proc(Host),
                            {complete_sync, Pid, SID, User, MeshID}),
            State;
        {_, Key} when Key =/= undefined ->
            Result = acknowledge_mailbox_keys(Host, [Key]),
            mailbox_audit_ack(Host, maps:get(jid, State, undefined),
                              maps:get(sid, State, undefined), direct, none,
                              [Key], Result),
            State;
        _ -> State
    end;
c2s_handle_send(State, _Packet, _SendResult) -> State.

%% ejabberd 26.04 invokes c2s_copy_session while migrating the old logical
%% XEP-0198 stream into a replacement c2s process.  The client supplies only
%% the opaque SM resume ID to mod_stream_mgmt; this hook derives all authority
%% identity from the server-owned old state and asks the serialized authority
%% owner to reserve the exact ready epoch.  A fresh bind never executes this
%% path and therefore remains unsynchronized.
c2s_copy_session(State, OldState) ->
    CleanState = maps:remove(?RESUME_META, State),
    case resume_handoff_identity(CleanState, OldState, self()) of
        {ok, Host, OldPid, OldSID, User, MeshID} ->
            case safe_call(Host, {prepare_resume, OldPid, OldSID, self(),
                                  User, MeshID}) of
                {ok, Token} -> CleanState#{?RESUME_META =>
                                               {Token, User, MeshID}};
                _ -> CleanState
            end;
        error -> CleanState
    end.

%% mod_stream_mgmt calls this only after the old state was copied, the new SID
%% was opened, and the <resumed/> plus complete old FIFO were sent.  Finalizing
%% synchronously makes readiness visible only for a successful replacement.
%% If a membership mutation raced the handoff, its serialized invalidation has
%% already deleted the ticket. The replayed snapshot-required control precedes
%% the resulting not-ready continuity result, which forces a clean bind.
c2s_session_resumed(#{lserver := Host, sid := NewSID,
                      jid := #jid{luser = User, lserver = Host,
                                  lresource = MeshID}} = State) ->
    %% Readiness and every outstanding processed-ack capability move together
    %% only through the server-generated one-use handoff ticket.  A failed or
    %% unrelated resumed hook cannot steal controls merely by naming the same
    %% user/resource.
    Status = case maps:get(?RESUME_META, State, undefined) of
                 {Token, User, MeshID} ->
                     try safe_call(Host,
                                   {complete_resume, self(), NewSID, User,
                                    MeshID, Token}) of
                         ready -> ready;
                         _ -> not_ready
                     catch _:_ -> not_ready
                     end;
                 _ -> not_ready
             end,
    CleanState = maps:remove(?RESUME_META, State),
    send_resume_authority_result(CleanState, Host, User, MeshID, Status);
c2s_session_resumed(State) -> maps:remove(?RESUME_META, State).

%% This one-way result is generated wholly from the exact authenticated c2s
%% state after the serialized readiness handoff. mod_stream_mgmt has already
%% emitted its post-replay <r/>, so that request separates stale unacknowledged
%% results from this reconnect's result without a client IQ that could overtake
%% the cumulative XEP-0198 FIFO.
send_resume_authority_result(State, Host, User, MeshID, Status)
  when Status =:= ready; Status =:= not_ready ->
    IQ = resume_authority_result(Host, User, MeshID, Status),
    try ejabberd_c2s:send(State, IQ)
    catch _:_ -> State
    end.

resume_authority_result(Host, User, MeshID, Status)
  when Status =:= ready; Status =:= not_ready ->
    EncodedStatus = case Status of
                        ready -> <<"ready">>;
                        not_ready -> <<"not-ready">>
                    end,
    ID = <<"cynapsa-resume-authority-",
           (integer_to_binary(erlang:unique_integer(
                                [positive, monotonic])))/binary>>,
    Result = #xmlel{name = <<"resume-authority">>,
                    attrs = [{<<"xmlns">>, ?AUTHORITY_NAMESPACE},
                             {<<"status">>, EncodedStatus}],
                    children = []},
    #iq{type = result, id = ID,
        from = jid:make(<<>>, Host, <<>>),
        to = jid:make(User, Host, MeshID),
        sub_els = [Result]}.

resume_handoff_identity(
  #{jid := #jid{luser = User, lserver = Host, lresource = MeshID}},
  #{jid := #jid{luser = User, lserver = Host, lresource = MeshID},
    sid := OldSID}, NewPid) ->
    case session_pid_from_sid(OldSID) of
        OldPid when is_pid(OldPid), OldPid =/= NewPid ->
            {ok, Host, OldPid, OldSID, User, MeshID};
        _ -> error
    end;
resume_handoff_identity(_State, _OldState, _NewPid) -> error.

session_pid_from_sid({_Timestamp, Pid}) when is_pid(Pid) -> Pid;
session_pid_from_sid(_) -> none.

%% Stock mod_offline may continue to own ordinary account traffic.  Cynapsa
%% frames are owned exclusively by the exact-resource mailbox and can never
%% fall into the bare-account spool.
offline_message_guard({_Action, #message{} = Packet} = Acc) ->
    case cynapsa_message_kind(Packet) of
        eligible -> stop;
        transient -> stop;
        malformed -> stop;
        ordinary -> Acc
    end;
offline_message_guard(Acc) -> Acc.

cynapsa_message_kind(#message{id = MessageID, type = chat,
                              subject = [], body = [],
                              thread = undefined,
                              sub_els = [#xmlel{} = Frame]}) ->
    case exact_aztm_frame(Frame) of
        true ->
            %% The server deliberately does not decode Core-owned CBOR.  A
            %% canonical outer message ID selects durable envelope custody;
            %% any other ID selects direct transient routing. Mellium may add
            %% a transport-local stanza ID when Core leaves it absent. The
            %% receiving Core independently requires envelope outer/inner ID
            %% equality and reserves canonical message IDs for envelopes.
            case canonical_message_id(MessageID) of
                true -> eligible;
                false -> transient
            end;
        false ->
            case element_has_aztm(Frame) of
                true -> malformed;
                false -> ordinary
            end
    end;
cynapsa_message_kind(#message{sub_els = Elements}) ->
    case lists:any(fun element_has_aztm/1, Elements) of
        true -> malformed;
        false -> ordinary
    end;
cynapsa_message_kind(_) -> ordinary.

%% Mellium assigns a stanza-local ID even when Core intentionally leaves the
%% outer ID empty. It is not an application correlation ID and must not cross
%% the server boundary; canonical message IDs are preserved only on envelopes.
strip_transient_transport_id(#message{} = Packet) ->
    Packet#message{id = <<>>};
strip_transient_transport_id(Packet) -> Packet.

exact_aztm_frame(#xmlel{name = <<"frame">>, attrs = Attrs,
                        children = [{xmlcdata, Data}]})
  when is_binary(Data), byte_size(Data) > 0 ->
    lists:sort(Attrs) =:= lists:sort([{<<"xmlns">>, ?AZTM_NAMESPACE},
                                     {<<"v">>, <<"1">>}])
        andalso canonical_raw_base64_shape(Data) =/= false;
exact_aztm_frame(_) -> false.

element_has_aztm(#xmlel{name = Name, attrs = Attrs, children = Children}) ->
    Name =:= <<"frame">>
        orelse xml_attr(<<"xmlns">>, Attrs) =:= ?AZTM_NAMESPACE
        orelse lists:any(fun element_has_aztm/1, Children);
element_has_aztm(_) -> false.

canonical_raw_base64_shape(Data) ->
    case canonical_raw_base64_bytes(Data) of
        false -> false;
        true ->
            case byte_size(Data) rem 4 of
                0 -> {true, <<>>};
                2 ->
                    case raw_base64_terminal_4_bits(binary:last(Data)) of
                        true -> {true, <<"==">>};
                        false -> false
                    end;
                3 ->
                    case raw_base64_terminal_2_bits(binary:last(Data)) of
                        true -> {true, <<"=">>};
                        false -> false
                    end;
                _ -> false
            end
    end.

canonical_raw_base64_bytes(<<>>) -> true;
canonical_raw_base64_bytes(<<C, Rest/binary>>)
  when (C >= $A andalso C =< $Z);
       (C >= $a andalso C =< $z);
       (C >= $0 andalso C =< $9);
       C =:= $+; C =:= $/ ->
    canonical_raw_base64_bytes(Rest);
canonical_raw_base64_bytes(_) -> false.

raw_base64_terminal_4_bits(C) ->
    C =:= $A orelse C =:= $Q orelse C =:= $g orelse C =:= $w.

raw_base64_terminal_2_bits(C) ->
    lists:member(C, "AEIMQUYcgkosw048").

%% Canonical 128-bit `msg_` identifiers use unpadded Base64URL.  Twenty-one
%% complete alphabet characters plus one four-bit terminal character encode
%% exactly 16 bytes; the restricted terminal alphabet rejects non-canonical
%% aliases with nonzero discarded bits.
canonical_message_id(<<"msg_", Prefix:21/binary, Last>>) ->
    canonical_base64url(Prefix)
        andalso (Last =:= $A orelse Last =:= $Q orelse
                 Last =:= $g orelse Last =:= $w);
canonical_message_id(_) -> false.

canonical_base64url(<<>>) -> true;
canonical_base64url(<<C, Rest/binary>>)
  when (C >= $A andalso C =< $Z);
       (C >= $a andalso C =< $z);
       (C >= $0 andalso C =< $9);
       C =:= $-; C =:= $_ ->
    canonical_base64url(Rest);
canonical_base64url(_) -> false.

store_mailbox_packet(#message{id = MessageID, from = From, to = To} = Packet,
                     Host,
                     MaxMessages, MaxBytes, RetentionSeconds) ->
    Owner = {To#jid.luser, Host, To#jid.lresource},
    Sender = {From#jid.luser, Host, From#jid.lresource},
    Clean = Packet#message{meta = #{}},
    try byte_size(fxml:element_to_binary(xmpp:encode(Clean))) of
        Bytes when Bytes > 0, Bytes =< MaxBytes ->
            Now = erlang:system_time(millisecond),
            Expires = Now + RetentionSeconds * 1000,
            Store = fun() ->
                            store_mailbox_packet_tx(
                              Clean, Owner, Sender, MessageID, Bytes, Now,
                              Expires,
                              MaxMessages, MaxBytes)
                    end,
            case mnesia:transaction(Store, [], 3) of
                {atomic, {ok, Key, StoredPacket}} ->
                    {ok, Key,
                     xmpp:put_meta(StoredPacket, ?MAILBOX_META, Key)};
                {aborted, mailbox_full} -> {error, full};
                {aborted, _} -> {error, storage}
            end;
        _ -> {error, full}
    catch _:_ -> {error, storage}
    end;
store_mailbox_packet(_, _Host, _MaxMessages, _MaxBytes, _Retention) ->
    {error, storage}.

store_mailbox_packet_tx(Packet, Owner, Sender, MessageID, Bytes, Now, Expires,
                        MaxMessages, MaxBytes) ->
    Matches = [Row || #cynapsa_mesh_mailbox{owner = ExistingOwner,
                                            sender = ExistingSender} = Row
                         <- mnesia:index_read(cynapsa_mesh_mailbox,
                                              MessageID, message_id),
                      ExistingOwner =:= Owner,
                      ExistingSender =:= Sender],
    case Matches of
        [#cynapsa_mesh_mailbox{key = ExistingKey,
                               packet = ExistingPacket,
                               expires_at = ExistingExpiry}]
          when ExistingExpiry > Now ->
            %% A lost custody receipt replays the same scoped identity. Keep
            %% the original FIFO row and bytes instead of consuming quota with
            %% copies; receiver-side ID-only dedupe remains the final guard.
            {ok, ExistingKey, ExistingPacket};
        [#cynapsa_mesh_mailbox{} = Expired] ->
            delete_mailbox_row_tx(Expired),
            store_new_mailbox_packet_tx(Packet, Owner, Sender, MessageID,
                                        Bytes, Now, Expires, MaxMessages,
                                        MaxBytes);
        [] ->
            store_new_mailbox_packet_tx(Packet, Owner, Sender, MessageID,
                                        Bytes, Now, Expires, MaxMessages,
                                        MaxBytes);
        _ -> mnesia:abort(duplicate_mailbox_identity)
    end.

store_new_mailbox_packet_tx(Packet, Owner, Sender, MessageID, Bytes, Now,
                            Expires, MaxMessages, MaxBytes) ->
    Cursor = case mnesia:read(cynapsa_mesh_mailbox_cursor, Owner, write) of
                 [] -> #cynapsa_mesh_mailbox_cursor{owner = Owner};
                 [#cynapsa_mesh_mailbox_cursor{} = Existing] -> Existing;
                 _ -> mnesia:abort(invalid_mailbox_cursor)
             end,
    Count = Cursor#cynapsa_mesh_mailbox_cursor.message_count,
    UsedBytes = Cursor#cynapsa_mesh_mailbox_cursor.byte_count,
    Sequence = Cursor#cynapsa_mesh_mailbox_cursor.next_sequence,
    case is_integer(Sequence) andalso Sequence > 0
         andalso Count < MaxMessages
         andalso Bytes =< MaxBytes - UsedBytes of
        true ->
            Key = {Owner, Sequence},
            case mnesia:read(cynapsa_mesh_mailbox, Key, write) of
                [] ->
                    mnesia:write(#cynapsa_mesh_mailbox{
                                    key = Key, owner = Owner,
                                    sender = Sender, message_id = MessageID,
                                    packet = Packet,
                                    bytes = Bytes, inserted_at = Now,
                                    expires_at = Expires}),
                    mnesia:write(Cursor#cynapsa_mesh_mailbox_cursor{
                                   next_sequence = Sequence + 1,
                                   message_count = Count + 1,
                                   byte_count = UsedBytes + Bytes}),
                    {ok, Key, Packet};
                _ -> mnesia:abort(mailbox_sequence_collision)
            end;
        false -> mnesia:abort(mailbox_full)
    end.

dispatch_mailbox_packet(Key, Packet,
                        #jid{luser = User, lserver = Host,
                             lresource = MeshID}, Ready) ->
    case current_session_pid(User, Host, MeshID) of
        Pid when is_pid(Pid) ->
            case maps:get(Pid, Ready, none) of
                {_SID, User, MeshID} ->
                    case ejabberd_c2s:route(
                           Pid,
                           {route, xmpp:put_meta(Packet, ?MAILBOX_META, Key)}) of
                        %% ejabberd_cluster:send/2 returning true proves only
                        %% enqueue into the live c2s Erlang process. TCP write,
                        %% recipient handling, and mailbox retirement remain
                        %% evidenced by the later resource-scoped SM ACK.
                        true -> c2s_enqueued;
                        _ -> queued
                    end;
                _ -> queued
            end;
        _ -> queued
    end.

dispatch_custody_accepted(#message{id = MessageID} = Packet,
                          #jid{luser = User, lserver = Host,
                               lresource = MeshID} = Sender,
                          Host, Ready) ->
    case {canonical_message_id(MessageID),
          xmpp:get_meta(Packet, ?SESSION_META, undefined)} of
        {true, {Pid, SID, Sender}} when is_pid(Pid), SID =/= undefined ->
            case exact_session_epoch_current(
                   Pid, SID, User, Host, MeshID, Ready,
                   fun current_session_pid/3) of
                true ->
                    Accepted = #xmlel{
                                  name = <<"accepted">>,
                                  attrs = [{<<"xmlns">>, ?CUSTODY_NAMESPACE},
                                           {<<"message-id">>, MessageID}]},
                    Receipt0 = #message{
                                  id = MessageID, type = headline,
                                  from = jid:make(<<>>, Host, <<>>),
                                  to = Sender, sub_els = [Accepted]},
                    Receipt = xmpp:put_meta(
                                Receipt0, ?CUSTODY_META,
                                {Pid, SID, Sender, MessageID}),
                    ejabberd_c2s:route(Pid, {route, Receipt}),
                    ok;
                false -> stale_sender
            end;
        _ -> stale_sender
    end;
dispatch_custody_accepted(_, _, _, _) -> invalid.

custody_receipt_for_session(Packet, {Pid, SID, BoundJID}, Host) ->
    case xmpp:get_meta(Packet, ?CUSTODY_META, undefined) of
        undefined -> none;
        {Pid, SID, BoundJID, MessageID} ->
            exact_custody_receipt(Packet, BoundJID, Host, MessageID);
        _ -> false
    end.

exact_custody_receipt(
  #message{id = MessageID, type = headline,
           from = From, to = To, subject = [], body = [], thread = undefined,
           sub_els = [#xmlel{name = <<"accepted">>, attrs = Attrs,
                            children = []}]},
  BoundJID, Host, MessageID) ->
    exact_authority_domain(From, Host)
        andalso jid:encode(To) =:= jid:encode(BoundJID)
        andalso canonical_full_jid(To)
        andalso canonical_message_id(MessageID)
        andalso lists:sort(Attrs) =:=
                  lists:sort([{<<"xmlns">>, ?CUSTODY_NAMESPACE},
                              {<<"message-id">>, MessageID}]);
exact_custody_receipt(_, _, _, _) -> false.

drain_mailbox(Host, User, MeshID, Pid, Ready) ->
    Owner = {User, Host, MeshID},
    case read_mailbox(Owner) of
        {ok, Rows} ->
            lists:foreach(
              fun(#cynapsa_mesh_mailbox{key = Key, packet = Packet}) ->
                      _ = dispatch_mailbox_packet(
                            Key, Packet, jid:make(User, Host, MeshID), Ready)
              end, Rows),
            case current_session_pid(User, Host, MeshID) of
                Pid -> ok;
                _ -> {error, session_replaced}
            end;
        Error -> Error
    end.

read_mailbox(Owner) ->
    Now = erlang:system_time(millisecond),
    Read = fun() ->
                   Rows = mnesia:index_read(cynapsa_mesh_mailbox, Owner,
                                            #cynapsa_mesh_mailbox.owner),
                   read_mailbox_rows_tx(lists:sort(Rows), Owner, Now, [])
           end,
    case mnesia:transaction(Read, [], 3) of
        {atomic, Rows} -> {ok, Rows};
        {aborted, _} -> {error, mailbox_storage_failure}
    end.

read_mailbox_rows_tx([], _Owner, _Now, Acc) -> lists:reverse(Acc);
read_mailbox_rows_tx([#cynapsa_mesh_mailbox{
                         key = {Owner, Sequence}, owner = Owner,
                         sender = {_FromUser, _Host, _MeshID},
                         packet = #message{} = Packet,
                         bytes = Bytes, inserted_at = Inserted,
                         expires_at = Expires} = Row | Rest],
                     Owner, Now, Acc)
  when is_integer(Sequence), Sequence > 0, is_integer(Bytes), Bytes > 0,
       is_integer(Inserted), is_integer(Expires), Expires > Inserted ->
    case Expires =< Now of
        true ->
            delete_mailbox_row_tx(Row),
            read_mailbox_rows_tx(Rest, Owner, Now, Acc);
        false ->
            case cynapsa_message_kind(Packet) of
                eligible -> read_mailbox_rows_tx(Rest, Owner, Now,
                                                 [Row | Acc]);
                _ -> mnesia:abort(invalid_mailbox_row)
            end
    end;
read_mailbox_rows_tx(_, _Owner, _Now, _Acc) ->
    mnesia:abort(invalid_mailbox_row).

acknowledged_mailbox_keys(Queue, Handled) ->
    try p1_queue:to_list(Queue) of
        Items -> acknowledged_mailbox_prefix(Items, Handled, [])
    catch _:_ -> []
    end.

%% XEP-0198 h is an unsigned 32-bit counter.  Numeric =< is incorrect across
%% wrap (for example, [..., 4294967295, 0] acknowledged by h=0).  The queue is
%% already in wire order, so retire only the prefix ending at the exact h.  If
%% h is stale and no longer present, no durable row is touched.
acknowledged_mailbox_prefix([], _Handled, _Keys) -> [];
acknowledged_mailbox_prefix([{Sequence, _Time, Packet} | Rest], Handled,
                            Keys) ->
    Key = xmpp:get_meta(Packet, ?MAILBOX_META, undefined),
    Updated = case Key of undefined -> Keys; _ -> [Key | Keys] end,
    case Sequence =:= Handled of
        true -> lists:usort(Updated);
        false -> acknowledged_mailbox_prefix(Rest, Handled, Updated)
    end;
acknowledged_mailbox_prefix(_, _Handled, _Keys) -> [].

acknowledge_mailbox_keys(_Host, []) -> ok;
acknowledge_mailbox_keys(Host, Keys) ->
    Delete = fun() ->
                     lists:reverse(
                       lists:foldl(
                         fun(Key, Deleted) ->
                                 case acknowledge_mailbox_key_tx(Host, Key) of
                                     deleted -> [Key | Deleted];
                                     absent -> Deleted
                                 end
                         end, [], Keys))
             end,
    case mnesia:transaction(Delete, [], 3) of
        {atomic, Deleted} -> {ok, Deleted};
        _ -> {error, mailbox_ack_storage_failure}
    end.

acknowledge_mailbox_key_tx(Host, Key) ->
    case mnesia:read(cynapsa_mesh_mailbox, Key, write) of
        [] -> absent;
        [#cynapsa_mesh_mailbox{owner = {_User, Host, _MeshID}} = Row] ->
            delete_mailbox_row_tx(Row),
            deleted;
        _ -> mnesia:abort(invalid_mailbox_row)
    end.

mailbox_audit_admission(Host,
                        #message{id = MessageID, from = From, to = To},
                        {_Owner, Sequence}, Dispatch, Custody) ->
    mailbox_audit(
      Host,
      "admission message_id=~ts sender=~ts recipient=~ts sequence=~B "
      "dispatch=~p custody=~p",
      [MessageID, jid:encode(From), jid:encode(To), Sequence, Dispatch,
       Custody]);
mailbox_audit_admission(_Host, _Packet, _Key, _Dispatch, _Custody) -> ok.

mailbox_audit_ack(Host, JID, SID, Mode, Handled, Keys, Result) ->
    case Keys of
        [] -> ok;
        _ ->
            mailbox_audit(
              Host,
              "ack recipient=~p sid=~p mode=~p handled=~p keys=~p result=~p",
              [mailbox_audit_jid(JID), SID, Mode, Handled, Keys, Result])
    end.

mailbox_audit_jid(#jid{} = JID) -> jid:encode(JID);
mailbox_audit_jid(_) -> undefined.

mailbox_audit(Host, Format, Arguments) ->
    case persistent_term:get(?MAILBOX_AUDIT_KEY(Host), false) of
        true -> ?INFO_MSG("cynapsa_mailbox " ++ Format, Arguments);
        false -> ok
    end.

delete_mailbox_row_tx(#cynapsa_mesh_mailbox{
                         key = Key, owner = Owner, bytes = Bytes} = Row)
  when is_integer(Bytes), Bytes > 0 ->
    case mnesia:read(cynapsa_mesh_mailbox_cursor, Owner, write) of
        [#cynapsa_mesh_mailbox_cursor{
            message_count = Count, byte_count = UsedBytes} = Cursor]
          when Count > 0, UsedBytes >= Bytes ->
            mnesia:delete_object(Row),
            mnesia:write(Cursor#cynapsa_mesh_mailbox_cursor{
                           message_count = Count - 1,
                           byte_count = UsedBytes - Bytes}),
            ok;
        _ -> mnesia:abort({invalid_mailbox_cursor, Key})
    end;
delete_mailbox_row_tx(_) -> mnesia:abort(invalid_mailbox_row).

prune_expired_mailbox(Host) ->
    Now = erlang:system_time(millisecond),
    Prune = fun() ->
                    mnesia:foldl(
                      fun(#cynapsa_mesh_mailbox{
                             owner = {_User, RowHost, _MeshID},
                             expires_at = Expires} = Row, Acc)
                            when RowHost =:= Host, is_integer(Expires) ->
                              case Expires =< Now of
                                  true -> delete_mailbox_row_tx(Row);
                                  false -> ok
                              end,
                              Acc;
                         (#cynapsa_mesh_mailbox{}, Acc) -> Acc;
                         (_, _Acc) -> mnesia:abort(invalid_mailbox_row)
                      end, ok, cynapsa_mesh_mailbox)
            end,
    case mnesia:transaction(Prune, [], 3) of
        {atomic, ok} -> ok;
        _ -> {error, mailbox_storage_failure}
    end.

reject_packet(Packet) ->
    AuthorityHost = persistent_term:get(?AUTHORITY_HOST_KEY, undefined),
    case denied_packet_response(Packet, AuthorityHost) of
        {route, ErrorPacket} -> ejabberd_router:route(ErrorPacket);
        drop -> ok
    end,
    drop.

reject_mailbox_packet(Packet, Reason) ->
    AuthorityHost = persistent_term:get(?AUTHORITY_HOST_KEY, undefined),
    case authenticated_client_origin(Packet, AuthorityHost,
                                     fun current_session_pid/3) of
        {ok, Pid, SID, BoundJID} ->
            Lang = xmpp:get_lang(Packet),
            Text = case Reason of
                       mailbox_full -> <<"Cynapsa resource mailbox is full">>;
                       _ -> <<"Cynapsa resource mailbox is unavailable">>
                   end,
            Error = xmpp:err_resource_constraint(Text, Lang),
            ServerJID = jid:make(<<>>, AuthorityHost, <<>>),
            ErrorPacket0 = set_packet_addresses(
                             xmpp:make_error(sanitize_denied_packet(Packet),
                                             Error),
                             ServerJID, BoundJID),
            ErrorPacket = xmpp:put_meta(
                            ErrorPacket0, ?ERROR_META,
                            {policy_error, Pid, SID, BoundJID}),
            ejabberd_router:route(ErrorPacket);
        _ -> ok
    end,
    drop.

%% A denied server/internal stanza must never be reflected.  In particular,
%% XEP-0280 and MAM/forwarded wrappers can contain attacker-controlled nested
%% payload and addresses.  Only the exact live c2s origin stamped by
%% user_send_packet receives a minimal policy error.
denied_packet_response(Packet, AuthorityHost) ->
    denied_packet_response(Packet, AuthorityHost,
                           fun current_session_pid/3).

denied_packet_response(Packet, AuthorityHost, SessionPid) ->
    case {xmpp:get_type(Packet),
          authenticated_client_origin(Packet, AuthorityHost, SessionPid)} of
        {error, _} -> drop;
        {result, _} -> drop;
        {_, {ok, Pid, SID, BoundJID}} ->
            Lang = xmpp:get_lang(Packet),
            Error = xmpp:err_policy_violation(
                      <<"Cynapsa mesh routing policy rejected the stanza">>, Lang),
            Sanitized = sanitize_denied_packet(Packet),
            ServerJID = jid:make(<<>>, AuthorityHost, <<>>),
            ErrorPacket0 = set_packet_addresses(
                             xmpp:make_error(Sanitized, Error),
                             ServerJID, BoundJID),
            ErrorPacket = xmpp:put_meta(
                            ErrorPacket0, ?ERROR_META,
                            {policy_error, Pid, SID, BoundJID}),
            {route, ErrorPacket};
        _ -> drop
    end.

authenticated_client_origin(Packet, AuthorityHost, SessionPid)
  when is_binary(AuthorityHost), is_function(SessionPid, 3) ->
    From = xmpp:get_from(Packet),
    case xmpp:get_meta(Packet, ?SESSION_META, undefined) of
        {Pid, SID, #jid{} = BoundJID}
          when is_pid(Pid), SID =/= undefined ->
            case canonical_full_jid(BoundJID)
                 andalso BoundJID#jid.lserver =:= AuthorityHost
                 andalso is_record(From, jid)
                 andalso exact_jid(From, BoundJID)
                 andalso safe_session_pid(SessionPid,
                                           BoundJID#jid.luser,
                                           BoundJID#jid.lserver,
                                           BoundJID#jid.lresource) =:= Pid of
                true -> {ok, Pid, SID, BoundJID};
                false -> error
            end;
        _ -> error
    end;
authenticated_client_origin(_Packet, _AuthorityHost, _SessionPid) -> error.

%% A membership control acknowledgement is consumed at the authenticated c2s
%% ingress and never enters ordinary server IQ routing. The outer ID selects
%% server-owned pending state; caller-supplied identity never does.
authority_control_ack_shape(#iq{id = ID, type = result, from = From, to = To,
                                sub_els = []}, BoundJID, Host)
  when is_binary(ID), is_binary(Host) ->
    case control_id(ID) of
        true ->
            case exact_jid(From, BoundJID)
                 andalso exact_authority_domain(To, Host) of
                true -> {ack, ID};
                false -> invalid
            end;
        false -> none
    end;
authority_control_ack_shape(#iq{id = ID}, _BoundJID, _Host)
  when is_binary(ID) ->
    case control_id(ID) of true -> invalid; false -> none end;
authority_control_ack_shape(_Packet, _BoundJID, _Host) -> none.

control_id(<<"cynapsa-membership-", Suffix/binary>>)
  when byte_size(Suffix) > 0, byte_size(Suffix) =< 96 ->
    lists:all(fun(Byte) ->
                      (Byte >= $a andalso Byte =< $z) orelse
                      (Byte >= $A andalso Byte =< $Z) orelse
                      (Byte >= $0 andalso Byte =< $9) orelse
                      Byte =:= $- orelse Byte =:= $_
              end, binary_to_list(Suffix));
control_id(_) -> false.

current_session_pid(User, Host, Resource) ->
    ejabberd_sm:get_session_pid(User, Host, Resource).

safe_session_pid(SessionPid, User, Host, Resource) ->
    try SessionPid(User, Host, Resource)
    catch _:_ -> none
    end.

sanitize_denied_packet(#message{} = Packet) ->
    Packet#message{subject = [], body = [], thread = undefined, sub_els = []};
sanitize_denied_packet(#iq{} = Packet) ->
    Packet#iq{sub_els = []};
sanitize_denied_packet(#presence{} = Packet) ->
    Packet#presence{show = undefined, status = [], priority = undefined,
                    sub_els = []}.

set_packet_addresses(#message{} = Packet, From, To) ->
    Packet#message{from = From, to = To};
set_packet_addresses(#iq{} = Packet, From, To) ->
    Packet#iq{from = From, to = To};
set_packet_addresses(#presence{} = Packet, From, To) ->
    Packet#presence{from = From, to = To}.

safe_policy_error(Packet, AuthorityHost) ->
    safe_policy_error(Packet, AuthorityHost, fun current_session_pid/3).

safe_policy_error(Packet, AuthorityHost, SessionPid) ->
    case xmpp:get_meta(Packet, ?ERROR_META, undefined) of
        {policy_error, Pid, SID, #jid{} = BoundJID} = Origin
          when is_pid(Pid), SID =/= undefined, is_binary(AuthorityHost) ->
            safe_policy_error_shape(Packet, AuthorityHost, BoundJID)
                andalso safe_session_pid(SessionPid,
                                          BoundJID#jid.luser,
                                          BoundJID#jid.lserver,
                                          BoundJID#jid.lresource) =:= Pid
                andalso Origin =:= {policy_error, Pid, SID, BoundJID};
        _ -> false
    end.

safe_policy_error_for_session(Packet, {Pid, SID, #jid{} = BoundJID},
                              AuthorityHost) ->
    safe_policy_error_for_session(Packet, {Pid, SID, BoundJID}, AuthorityHost,
                                  fun current_session_pid/3);
safe_policy_error_for_session(_Packet, _Session, _AuthorityHost) -> false.

safe_policy_error_for_session(Packet, {Pid, SID, #jid{} = BoundJID},
                              AuthorityHost, SessionPid)
  when is_function(SessionPid, 3) ->
    xmpp:get_meta(Packet, ?ERROR_META, undefined)
        =:= {policy_error, Pid, SID, BoundJID}
        andalso safe_policy_error_shape(Packet, AuthorityHost, BoundJID)
        andalso safe_session_pid(SessionPid, BoundJID#jid.luser,
                                 BoundJID#jid.lserver,
                                 BoundJID#jid.lresource) =:= Pid;
safe_policy_error_for_session(_Packet, _Session, _AuthorityHost,
                              _SessionPid) -> false.

safe_policy_error_shape(Packet, AuthorityHost, BoundJID) ->
    ServerJID = jid:make(<<>>, AuthorityHost, <<>>),
    xmpp:get_type(Packet) =:= error
        andalso canonical_full_jid(BoundJID)
        andalso BoundJID#jid.lserver =:= AuthorityHost
        andalso exact_jid(xmpp:get_to(Packet), BoundJID)
        andalso exact_authority_domain(xmpp:get_from(Packet), AuthorityHost)
        andalso jid:encode(xmpp:get_from(Packet)) =:= jid:encode(ServerJID)
        andalso sanitized_error_payload(Packet).

sanitized_error_payload(#message{subject = [], body = [], thread = undefined,
                                 sub_els = [#stanza_error{}]}) -> true;
sanitized_error_payload(#iq{sub_els = [#stanza_error{}]}) -> true;
sanitized_error_payload(#presence{show = undefined, status = [],
                                  priority = undefined,
                                  sub_els = [#stanza_error{}]}) -> true;
sanitized_error_payload(_) -> false.

%% This is the only hook that is guaranteed to identify a client-originated
%% stanza before archive/offline hooks can retain it.  The authenticated c2s
%% identity is authoritative: a stanza cannot supply an alias, omit its full
%% sender, or select another resource.  The configured canonical authority
%% host and explicit service payload allowlist are the only non-agent route;
%% no empty-localpart identity is accepted merely because it is bare.
client_packet_allowed(Packet, BoundJID, AuthorityHost) ->
    case {client_stanza(Packet), canonical_full_jid(BoundJID),
          xmpp:get_from(Packet), xmpp:get_to(Packet)} of
        {true, true, #jid{} = From, #jid{} = To} ->
            exact_jid(From, BoundJID)
                andalso client_payload_safe(Packet)
                andalso client_route_allowed(Packet, From, To, AuthorityHost);
        _ -> false
    end.

client_stanza(#message{}) -> true;
client_stanza(#iq{}) -> true;
client_stanza(#presence{}) -> true;
client_stanza(_) -> false.

client_route_allowed(Packet, From, To, AuthorityHost) ->
    case route_scope(From, To, AuthorityHost) of
        service -> approved_service_packet(Packet, From, To, AuthorityHost);
        upload_service -> approved_upload_request(Packet, From, To,
                                                  AuthorityHost);
        {agent, _, _, _, _} -> true;
        deny -> false
    end.

exact_jid(#jid{} = Left, #jid{} = Right) ->
    canonical_full_jid(Left) andalso canonical_full_jid(Right)
        andalso jid:encode(Left) =:= jid:encode(Right).

canonical_full_jid(#jid{user = User, server = Server, resource = Resource,
                        luser = User, lserver = Server,
                        lresource = Resource})
  when User =/= <<>>, Server =/= <<>>, Resource =/= <<>> ->
    canonical_user(User) =:= {ok, User}
        andalso canonical_host(Server) =:= {ok, Server}
        andalso canonical_mesh_id(Resource) =:= {ok, Resource};
canonical_full_jid(_) -> false.

%% Carbon copies, forwarded/MAM results, and nested standard stanzas are
%% server-originated containers.  A client may still use the ordinary carbon
%% enable/disable IQ service flow, but it cannot manufacture a wrapper and
%% route a second hidden stanza through an otherwise valid outer address.
client_payload_safe(Packet) ->
    try xmpp:encode(Packet) of
        #xmlel{children = Children} ->
            not lists:any(fun(Element) ->
                                  forbidden_wrapped_element(
                                    Element, <<"jabber:client">>)
                          end, Children);
        _ -> false
    catch
        _:_ -> false
    end.

forbidden_wrapped_element(#xmlel{name = Name, attrs = Attrs,
                                 children = Children}, ParentNamespace) ->
    Namespace = case xml_attr(<<"xmlns">>, Attrs) of
                    undefined -> ParentNamespace;
                    Value -> Value
                end,
    prefixed_element(Name)
        orelse prefixed_namespace_declaration(Attrs)
        orelse forbidden_wrapper(Name, Namespace)
        orelse lists:any(fun(Element) ->
                                 forbidden_wrapped_element(Element, Namespace)
                         end, Children);
forbidden_wrapped_element(_, _ParentNamespace) -> false.

prefixed_element(Name) when is_binary(Name) ->
    binary:match(Name, <<":">>) =/= nomatch;
prefixed_element(_) -> true.

prefixed_namespace_declaration(Attrs) ->
    lists:any(fun({<<"xmlns:", _/binary>>, _Value}) -> true;
                 (_) -> false
              end, Attrs).

forbidden_wrapper(<<"forwarded">>, <<"urn:xmpp:forward:0">>) -> true;
forbidden_wrapper(<<"sent">>, <<"urn:xmpp:carbons:2">>) -> true;
forbidden_wrapper(<<"received">>, <<"urn:xmpp:carbons:2">>) -> true;
forbidden_wrapper(<<"result">>, <<"urn:xmpp:mam:", _/binary>>) -> true;
forbidden_wrapper(Name, Namespace)
  when Name =:= <<"message">>; Name =:= <<"iq">>; Name =:= <<"presence">> ->
    Namespace =:= <<"jabber:client">> orelse Namespace =:= <<"jabber:server">>;
forbidden_wrapper(_, _) -> false.

xml_attr(Name, Attrs) ->
    case lists:keyfind(Name, 1, Attrs) of
        {Name, Value} -> Value;
        false -> undefined
    end.

route_scope(_From, _To, undefined) -> deny;
route_scope(#jid{} = From, #jid{} = To, AuthorityHost) ->
    case canonical_host(AuthorityHost) of
        {ok, AuthorityHost} -> route_scope_canonical(From, To, AuthorityHost);
        _ -> deny
    end;
route_scope(_From, _To, _AuthorityHost) -> deny.

route_scope_canonical(From, To, Host) ->
    case upload_route_scope(From, To, Host) of
        upload_service -> upload_service;
        upload_deny -> deny;
        not_upload ->
            case {exact_authority_domain(From, Host),
                  exact_authority_domain(To, Host)} of
                {true, true} -> service;
                {true, false} ->
                    case canonical_local_full_jid(To, Host) of
                        true -> service;
                        false -> deny
                    end;
                {false, true} ->
                    case canonical_local_full_jid(From, Host) of
                        true -> service;
                        false -> deny
                    end;
                {false, false} -> agent_route_scope(From, To, Host)
            end
    end.

upload_route_scope(From, To, Host) ->
    case {exact_upload_domain(From, Host), exact_upload_domain(To, Host)} of
        {true, true} -> upload_deny;
        {true, false} ->
            case canonical_local_full_jid(To, Host) of
                true -> upload_service;
                false -> upload_deny
            end;
        {false, true} ->
            case canonical_local_full_jid(From, Host) of
                true -> upload_service;
                false -> upload_deny
            end;
        {false, false} -> not_upload
    end.

agent_route_scope(#jid{luser = FromUser, lserver = Host,
                       lresource = FromResource} = From,
                  #jid{luser = ToUser, lserver = Host,
                       lresource = ToResource} = To, Host) ->
    case {canonical_full_jid(From), canonical_full_jid(To),
          FromResource, ToResource, FromResource =:= ToResource,
          canonical_mesh_id(FromResource)} of
        {false, _, _, _, _, _} -> deny;
        {_, false, _, _, _, _} -> deny;
        {_, _, <<>>, _, _, _} -> deny;
        {_, _, _, <<>>, _, _} -> deny;
        {_, _, _, _, false, _} -> deny;
        {_, _, _, _, true, {ok, MeshID}} ->
            {agent, Host, MeshID, {FromUser, Host}, {ToUser, Host}};
        _ -> deny
    end;
agent_route_scope(_From, _To, _AuthorityHost) -> deny.

exact_authority_domain(
  #jid{user = <<>>, server = Host, resource = <<>>,
       luser = <<>>, lserver = Host, lresource = <<>>} = JID, Host)
  when is_binary(Host) ->
    canonical_host(Host) =:= {ok, Host} andalso jid:encode(JID) =:= Host;
exact_authority_domain(_, _) -> false.

upload_host(Host) ->
    case canonical_host(Host) of
        {ok, Host} ->
            UploadHost = <<?UPLOAD_PREFIX/binary, Host/binary>>,
            case canonical_host(UploadHost) of
                {ok, UploadHost} -> {ok, UploadHost};
                _ -> error
            end;
        _ -> error
    end.

exact_upload_domain(
  #jid{user = <<>>, server = UploadHost, resource = <<>>,
       luser = <<>>, lserver = UploadHost, lresource = <<>>} = JID, Host) ->
    upload_host(Host) =:= {ok, UploadHost}
        andalso jid:encode(JID) =:= UploadHost;
exact_upload_domain(_, _) -> false.

canonical_local_full_jid(#jid{lserver = Host} = JID, Host) ->
    canonical_full_jid(JID);
canonical_local_full_jid(_, _) -> false.

approved_service_packet(#iq{} = IQ, From, To, Host) ->
    exact_service_endpoints(From, To, Host)
        andalso client_payload_safe(IQ)
        andalso approved_service_iq(IQ);
approved_service_packet(#presence{} = Presence, From, To, Host) ->
    exact_service_endpoints(From, To, Host)
        andalso client_payload_safe(Presence);
approved_service_packet(_Packet, _From, _To, _Host) -> false.

exact_service_endpoints(From, To, Host) ->
    route_scope(From, To, Host) =:= service.

approved_service_iq(#iq{type = Type, sub_els = Elements})
  when Type =:= get; Type =:= set ->
    length(Elements) =:= 1
        andalso lists:all(fun approved_service_element/1, Elements);
approved_service_iq(#iq{type = result, sub_els = Elements}) ->
    length(Elements) =< 1
        andalso lists:all(fun approved_service_element/1, Elements);
approved_service_iq(#iq{type = error, sub_els = Elements}) ->
    length(Elements) >= 1 andalso length(Elements) =< 2
        andalso length([Error || #stanza_error{} = Error <- Elements]) =:= 1
        andalso lists:all(fun(Element) ->
                                  is_record(Element, stanza_error)
                                      orelse approved_service_element(Element)
                          end, Elements);
approved_service_iq(_) -> false.

approved_service_element(#xmlel{name = Name, attrs = Attrs}) ->
    approved_service_name(Name, xml_attr(<<"xmlns">>, Attrs));
approved_service_element(Element) ->
    try xmpp:encode(Element) of
        #xmlel{name = Name, attrs = Attrs} ->
            approved_service_name(Name, xml_attr(<<"xmlns">>, Attrs));
        _ -> false
    catch
        _:_ -> false
    end.

approved_service_name(<<"sync">>, ?AUTHORITY_NAMESPACE) -> true;
approved_service_name(<<"synchronized">>, ?AUTHORITY_NAMESPACE) -> true;
approved_service_name(<<"membership-changed">>, ?AUTHORITY_NAMESPACE) -> true;
approved_service_name(<<"time">>, ?TIME_NAMESPACE) -> true;
approved_service_name(<<"query">>, ?DISCO_INFO_NAMESPACE) -> true;
approved_service_name(<<"query">>, ?DISCO_ITEMS_NAMESPACE) -> true;
approved_service_name(<<"services">>, ?EXTDISCO_NAMESPACE) -> true;
approved_service_name(<<"credentials">>, ?EXTDISCO_NAMESPACE) -> true;
approved_service_name(<<"ping">>, ?PING_NAMESPACE) -> true;
approved_service_name(Name, ?CARBONS_NAMESPACE)
  when Name =:= <<"enable">>; Name =:= <<"disable">> -> true;
approved_service_name(_, _) -> false.

%% XEP-0363 is the only auxiliary service identity.  It is deliberately not
%% part of the primary server/service allowlist: both its exact address and
%% its direction-specific wire shape are checked here.
approved_upload_request(#iq{type = get} = IQ, From, To, Host) ->
    canonical_local_full_jid(From, Host)
        andalso exact_upload_domain(To, Host)
        andalso valid_upload_request_iq(IQ) =/= error;
approved_upload_request(_Packet, _From, _To, _Host) -> false.

authorize_upload_route(Packet, From, To, Host) ->
    case {canonical_local_full_jid(From, Host),
          exact_upload_domain(To, Host),
          exact_upload_domain(From, Host),
          canonical_local_full_jid(To, Host)} of
        {true, true, false, false} ->
            case approved_upload_request(Packet, From, To, Host) of
                true -> case safe_call(Host, {upload_request, Packet}) of
                            {ok, Meta} -> {ok, Meta};
                            _ -> error
                        end;
                false -> error
            end;
        {false, false, true, true} ->
            case valid_upload_response_iq(Packet) of
                {ok, _ID} -> case safe_call(Host, {upload_response, Packet}) of
                                 {ok, Meta} -> {ok, Meta};
                                 _ -> error
                             end;
                error -> error
            end;
        _ -> error
    end.

register_upload_request(Host, Packet, Ready, Pending, Now) ->
    case {authenticated_client_origin(Packet, Host,
                                      fun current_session_pid/3),
          valid_upload_request_iq(Packet)} of
        {{ok, Pid, SID,
          #jid{luser = User, lresource = MeshID} = BoundJID},
         {ok, ID, Size}} ->
            case maps:get(Pid, Ready, none) of
                {SID, User, MeshID} ->
                    register_upload_id(Pid, SID, BoundJID, ID, Size,
                                       Pending, Now);
                _ -> error
            end;
        _ -> error
    end.

register_upload_id(Pid, SID, BoundJID, ID, Size, Pending, Now) ->
    IDs0 = case maps:get(Pid, Pending, undefined) of
               {SID, BoundJID, Existing} -> purge_upload_ids(Existing, Now);
               undefined -> #{};
               _ -> #{}
           end,
    case maps:is_key(ID, IDs0) orelse map_size(IDs0) >= ?MAX_UPLOAD_PENDING of
        true -> error;
        false ->
            Expiry = Now + ?UPLOAD_PENDING_MILLISECONDS,
            {ok, Random} = valid_upload_id(ID),
            Filename = <<?UPLOAD_FILENAME_PREFIX/binary, Random/binary,
                         ?UPLOAD_FILENAME_SUFFIX/binary>>,
            IDs = IDs0#{ID => {Expiry, Filename, Size}},
            Meta = {upload_request, Pid, SID, BoundJID, ID},
            {ok, Meta, Pending#{Pid => {SID, BoundJID, IDs}}}
    end.
consume_upload_response(Host, Packet, Ready, Pending, Now) ->
    From = xmpp:get_from(Packet),
    To = xmpp:get_to(Packet),
    case {exact_upload_domain(From, Host),
          canonical_local_full_jid(To, Host),
          valid_upload_response_iq(Packet)} of
        {true, true, {ok, ID}} ->
            #jid{luser = User, lresource = MeshID} = To,
            Pid = current_session_pid(User, Host, MeshID),
            consume_upload_id(Pid, To, ID, Host, User, MeshID, Ready,
                              Pending, Now, Packet);
        _ -> error
    end.

consume_upload_id(Pid, BoundJID, ID, Host, User, MeshID, Ready,
                  Pending, Now, Packet) when is_pid(Pid) ->
    case member_authorized(Host, MeshID, {User, Host}) of
        true -> consume_upload_id_owned(Pid, BoundJID, ID,
                                        {User, MeshID}, Ready, Pending, Now,
                                        Packet);
        false -> error
    end;
consume_upload_id(_Pid, _BoundJID, _ID, _Host, _User, _MeshID, _Ready,
                  _Pending, _Now, _Packet) -> error.

consume_upload_id_owned(Pid, BoundJID, ID, {User, MeshID},
                        Ready, Pending, Now, Packet) ->
    case maps:get(Pid, Pending, undefined) of
        {SID, BoundJID, Existing} ->
            IDs = purge_upload_ids(Existing, Now),
            case {maps:take(ID, IDs), maps:get(Pid, Ready, none)} of
                {{{_Expiry, Filename, Size}, Rest}, {SID, User, MeshID}} ->
                    case track_upload_response(Packet, BoundJID, ID,
                                               Filename, Size) of
                        ok ->
                            Updated = update_upload_pending(
                                        Pid, SID, BoundJID, Rest, Pending),
                            Meta = {upload_response, Pid, SID, BoundJID, ID},
                            {ok, Meta, Updated};
                        error -> error
                    end;
                _ -> error
            end;
        _ -> error
    end.

update_upload_pending(Pid, _SID, _BoundJID, IDs, Pending)
  when map_size(IDs) =:= 0 -> maps:remove(Pid, Pending);
update_upload_pending(Pid, SID, BoundJID, IDs, Pending) ->
    Pending#{Pid => {SID, BoundJID, IDs}}.

purge_upload_ids(IDs, Now) ->
    maps:filter(fun(_ID, {Expiry, Filename, Size}) ->
                        is_integer(Expiry) andalso Expiry > Now
                            andalso is_binary(Filename)
                            andalso byte_size(Filename) > 0
                            andalso is_integer(Size) andalso Size > 0
                            andalso Size =< ?MAX_UPLOAD_BYTES;
                   (_ID, _Invalid) -> false
                end,
                IDs).

track_upload_response(#iq{type = error}, _BoundJID, _ID, _Filename,
                      _Size) -> ok;
track_upload_response(#iq{type = result} = Packet,
                      #jid{luser = User, lserver = Host,
                           lresource = MeshID}, ID, Filename, Size) ->
    case upload_put_url(Packet) of
        {ok, URL} ->
            case local_upload_object(Host, URL, Filename) of
                {ok, Path, Slot} ->
                    Key = upload_object_key(Host, Path, ID),
                    upload_object_admit(User, Host, MeshID, Key, Path, Slot,
                                        Size);
                error -> error
            end;
        error -> error
    end;
track_upload_response(_, _BoundJID, _ID, _Filename, _Size) -> error.

upload_object_key(Host, Path) -> {Host, Path}.
upload_object_key(Host, Path, _ClientID) -> upload_object_key(Host, Path).

upload_object_admit(User, Host, MeshID, Key, Path, Slot, Size) ->
    OwnerID = {User, Host, MeshID},
    case member_authorized(Host, MeshID, {User, Host}) of
        true -> upload_object_admit_authorized(OwnerID, Key, Path, Slot, Size);
        false -> error
    end.

upload_object_admit_authorized({User, Host, MeshID} = OwnerID,
                               Key, Path, Slot, Size) ->
    case prune_expired_uploads(Host) of
        ok ->
            %% Supported membership mutations and upload responses execute in
            %% this host's gen_server.  Recheck after pruning and immediately
            %% before admission so a removal can never leave a late owner row.
            case member_authorized(Host, MeshID, {User, Host}) of
                true -> upload_object_admit_current(
                          OwnerID, Key, Path, Slot, Size);
                false -> error
            end;
        _ -> error
    end.

upload_object_admit_current({User, Host, MeshID} = OwnerID,
                            Key, Path, Slot, Size) ->
    Token = persistent_term:get(?UPLOAD_INSTANCE_KEY, undefined),
    Expiry = erlang:monotonic_time(millisecond) + ?UPLOAD_SLOT_MILLISECONDS,
    Object = #cynapsa_mesh_upload_object{
               key = Key,
               owner = {User, Host, MeshID, pending, Size, Token, Expiry},
               path = Path,
               slot = Slot},
    Admit = fun() ->
                    %% Prevent write skew between concurrent admissions on
                    %% different object keys.
                    mnesia:write_lock_table(cynapsa_mesh_upload_object),
                    {Count, Bytes, OwnerCount, OwnerBytes} =
                        retained_upload_usage_tx(OwnerID),
                    case {mnesia:read(cynapsa_mesh_upload_object, Key, write),
                          retained_upload_capacity(
                            Count, Bytes, OwnerCount, OwnerBytes, Size)} of
                        {[], true} -> mnesia:write(Object), ok;
                        {[_], _} -> mnesia:abort(upload_path_collision);
                        {_, false} -> mnesia:abort(upload_object_capacity)
                    end
            end,
    case mnesia:transaction(Admit, [], 3) of
        {atomic, ok} -> ok;
        _ -> error
    end.

retained_upload_capacity(Count, Bytes, OwnerCount, OwnerBytes, Size) ->
    Count < ?MAX_RETAINED_UPLOAD_OBJECTS
        andalso Bytes + Size =< ?MAX_RETAINED_UPLOAD_BYTES
        andalso OwnerCount < ?MAX_OWNER_UPLOAD_OBJECTS
        andalso OwnerBytes + Size =< ?MAX_OWNER_UPLOAD_BYTES.

retained_upload_usage_tx(OwnerID) ->
    mnesia:foldl(
      fun(#cynapsa_mesh_upload_object{owner = Owner},
          {Count, Bytes, OwnerCount, OwnerBytes}) ->
              case {upload_owner_identity(Owner), upload_owner_size(Owner)} of
                  {OwnerID, {ok, Size}} ->
                      {Count + 1, Bytes + Size,
                       OwnerCount + 1, OwnerBytes + Size};
                  {Identity, {ok, Size}} when Identity =/= invalid ->
                      {Count + 1, Bytes + Size, OwnerCount, OwnerBytes};
                  _ -> mnesia:abort(invalid_upload_object)
              end;
         (_, _Acc) -> mnesia:abort(invalid_upload_object)
      end, {0, 0, 0, 0}, cynapsa_mesh_upload_object).

upload_owner_size({_User, _Host, _MeshID, pending, Size, Token, Expiry})
  when is_integer(Size), Size > 0, Size =< ?MAX_UPLOAD_BYTES,
       is_binary(Token), byte_size(Token) =:= 16,
       is_integer(Expiry) -> {ok, Size};
upload_owner_size({_User, _Host, _MeshID, completed, Size})
  when is_integer(Size), Size > 0, Size =< ?MAX_UPLOAD_BYTES -> {ok, Size};
upload_owner_size({_User, _Host, _MeshID, cleanup_pending, Size})
  when is_integer(Size), Size > 0, Size =< ?MAX_UPLOAD_BYTES -> {ok, Size};
%% A pre-migration row has unknown declared size.  Charge it at the maximum
%% rather than undercounting retained disk authority.
upload_owner_size({_User, _Host, _MeshID}) -> {ok, ?MAX_UPLOAD_BYTES};
upload_owner_size(_) -> error.

upload_owner_user({User, _Host, _MeshID, pending, _Size, _Token, _Expiry}) ->
    User;
upload_owner_user({User, _Host, _MeshID, completed, _Size}) -> User;
upload_owner_user({User, _Host, _MeshID, cleanup_pending, _Size}) -> User.

upload_owner_mesh({_User, _Host, MeshID, pending, _Size, _Token, _Expiry}) ->
    MeshID;
upload_owner_mesh({_User, _Host, MeshID, completed, _Size}) -> MeshID;
upload_owner_mesh({_User, _Host, MeshID, cleanup_pending, _Size}) -> MeshID.

upload_owner_http_allowed(
  {_User, Host, _MeshID, pending, _Size, Token, Expiry}, Host, 'PUT') ->
    Token =:= persistent_term:get(?UPLOAD_INSTANCE_KEY, undefined)
        andalso is_integer(Expiry)
        andalso Expiry > erlang:monotonic_time(millisecond);
upload_owner_http_allowed(
  {_User, Host, _MeshID, completed, _Size}, Host, Method)
  when Method =:= 'GET'; Method =:= 'HEAD' -> true;
upload_owner_http_allowed(_, _Host, _Method) -> false.

prune_expired_uploads(Host) ->
    Token = persistent_term:get(?UPLOAD_INSTANCE_KEY, undefined),
    Now = erlang:monotonic_time(millisecond),
    Read = fun() ->
                   mnesia:foldl(
                     fun(#cynapsa_mesh_upload_object{} = Object, Acc) ->
                             case upload_object_expired(Object, Host, Token,
                                                        Now) of
                                 true -> [Object | Acc];
                                 false -> Acc;
                                 error -> mnesia:abort(invalid_upload_object)
                             end;
                        (_, _Acc) -> mnesia:abort(invalid_upload_object)
                     end, [], cynapsa_mesh_upload_object)
           end,
    case mnesia:transaction(Read, [], 3) of
        {atomic, Objects} ->
            cleanup_reply([cleanup_expired_upload_object(
                             Object, Host, Token, Now)
                           || Object <- Objects]);
        _ -> {error, upload_ownership_storage_failure}
    end.

upload_object_expired(
  #cynapsa_mesh_upload_object{
    owner = {_User, Host, _MeshID, pending, _Size, Token, Expiry}},
  Host, CurrentToken, Now) when is_binary(Token), is_integer(Expiry) ->
    Token =/= CurrentToken orelse Expiry =< Now;
upload_object_expired(
  #cynapsa_mesh_upload_object{
    owner = {_User, Host, _MeshID, completed, _Size}}, Host, _Token, _Now) ->
    false;
upload_object_expired(
  #cynapsa_mesh_upload_object{
    owner = {_User, Host, _MeshID, cleanup_pending, _Size}}, Host,
  _Token, _Now) -> true;
upload_object_expired(
  #cynapsa_mesh_upload_object{owner = {_User, Host, _MeshID}}, Host,
  _Token, _Now) -> false;
upload_object_expired(#cynapsa_mesh_upload_object{}, _Host, _Token, _Now) ->
    error.

cleanup_expired_upload_object(
  #cynapsa_mesh_upload_object{path = Path, slot = Slot} = Object,
  Host, Token, Now) ->
    %% Path and slot are persisted authority data, not trusted filesystem
    %% inputs.  Invalid rows remain quota-charged for repair/removal retry.
    case catch persisted_upload_cleanup_safe(Object, Host) of
        true -> cleanup_valid_expired_upload_object(
                  Object, Host, Path, Slot, Token, Now);
        _ -> {error, invalid_upload_object_path}
    end.

persisted_upload_cleanup_safe(
  #cynapsa_mesh_upload_object{key = Key, owner = Owner, path = Path,
                              slot = Slot}, Host) ->
    case upload_owner_identity(Owner) of
        {_User, Host, _MeshID} ->
            Key =:= upload_object_key(Host, Path)
                andalso safe_upload_object_path(Host, Path, Slot);
        _ -> false
    end.

cleanup_valid_expired_upload_object(Object, Host, Path, Slot, Token, Now) ->
    case with_upload_path_lock(
           Path,
           fun() ->
                   case catch mnesia:dirty_read(
                                cynapsa_mesh_upload_object,
                                Object#cynapsa_mesh_upload_object.key) of
                       [Object] ->
                           case upload_object_expired(Object, Host, Token,
                                                      Now) of
                               true ->
                                   case cleanup_upload_object_locked(
                                          Host, Path, Slot) of
                                       ok -> delete_upload_record(Object);
                                       Error -> Error
                                   end;
                               _ -> ok
                           end;
                       _ -> ok
                   end
           end) of
        {ok, Result} -> Result;
        {error, _} -> {error, upload_path_lock_failed}
    end.

delete_upload_record(Object) ->
    case mnesia:transaction(fun() -> mnesia:delete_object(Object) end,
                            [], 3) of
        {atomic, ok} -> ok;
        _ -> {error, upload_ownership_storage_failure}
    end.

cancel_upload_response(#iq{type = result, id = ID} = Packet, Host) ->
    case {valid_upload_id(ID), upload_put_url(Packet)} of
        {{ok, Random}, {ok, URL}} ->
            Filename = <<?UPLOAD_FILENAME_PREFIX/binary, Random/binary,
                         ?UPLOAD_FILENAME_SUFFIX/binary>>,
            case local_upload_object(Host, URL, Filename) of
                {ok, Path, Slot} -> cancel_unowned_upload_object(
                                      Host, Path, Slot);
                error -> error
            end;
        _ -> error
    end;
cancel_upload_response(_Packet, _Host) -> ok.

cancel_unowned_upload_object(Host, Path, Slot) ->
    case safe_upload_object_path(Host, Path, Slot) of
        true ->
            case with_upload_path_lock(
                   Path,
                   fun() ->
                           Key = upload_object_key(Host, Path),
                           Rows = catch mnesia:dirty_read(
                                          cynapsa_mesh_upload_object, Key),
                           cancel_unowned_upload_path_locked(
                             Rows, Key, Path,
                             fun() -> cleanup_upload_object_locked(
                                        Host, Path, Slot)
                             end)
                   end) of
                {ok, Result} -> Result;
                {error, _} -> {error, upload_path_lock_failed}
            end;
        false -> {error, invalid_upload_object_path}
    end.

cancel_unowned_upload_path_locked([], _Key, _Path, Cleanup) -> Cleanup();
cancel_unowned_upload_path_locked(
  [#cynapsa_mesh_upload_object{key = Key, path = Path}], Key, Path,
  _Cleanup) -> ok;
cancel_unowned_upload_path_locked(_Rows, _Key, _Path, _Cleanup) -> error.

upload_put_url(#iq{sub_els = [Element]}) ->
    case upload_xmlel(Element) of
        {ok, #xmlel{name = <<"slot">>, children = Children}} ->
            upload_put_url_children(Children);
        _ -> error
    end;
upload_put_url(_) -> error.

upload_put_url_children([#xmlel{name = <<"put">>, attrs = Attrs} | _]) ->
    case exact_attrs(Attrs, [<<"url">>]) of
        {ok, #{<<"url">> := URL}} -> {ok, URL};
        _ -> error
    end;
upload_put_url_children([_ | Rest]) -> upload_put_url_children(Rest);
upload_put_url_children([]) -> error.

local_upload_object(Host, URL, Filename) ->
    Put0 = mod_http_upload_opt:put_url(Host),
    Put1 = mod_http_upload:expand_host(Put0, Host),
    Put = str:strip(Put1, right, $/),
    Prefix = <<Put/binary, $/>>,
    PrefixSize = byte_size(Prefix),
    case URL of
        <<Candidate:PrefixSize/binary, Relative/binary>>
          when Candidate =:= Prefix ->
            case binary:split(Relative, <<"?">>, [global]) of
                [PathPart] -> local_upload_slot(Host, PathPart, Filename);
                _ -> error
            end;
        _ -> error
    end.

local_upload_slot(Host, PathPart, Filename) ->
    case binary:split(PathPart, <<"/">>, [global]) of
        [UserDir, RandDir, Filename] = Slot ->
            case valid_upload_path_component(UserDir)
                 andalso valid_upload_path_component(RandDir) of
                true ->
                    DocRoot0 = mod_http_upload_opt:docroot(Host),
                    DocRoot1 = mod_http_upload:expand_home(DocRoot0),
                    DocRoot = mod_http_upload:expand_host(DocRoot1, Host),
                    {ok, str:join([DocRoot | Slot], <<$/>>), Slot};
                false -> error
            end;
        _ -> error
    end.

valid_upload_path_component(Value)
  when is_binary(Value), byte_size(Value) > 0, byte_size(Value) =< 256 ->
    lists:all(fun(Byte) ->
                      (Byte >= $a andalso Byte =< $z)
                          orelse (Byte >= $A andalso Byte =< $Z)
                          orelse (Byte >= $0 andalso Byte =< $9)
                  end, binary_to_list(Value));
valid_upload_path_component(_) -> false.

%% Authority synchronization and upload correlation share this gen_server.
%% A fresh exact-session snapshot preserves only unexpired leases already
%% owned by that exact session; replacement sessions cannot inherit them.
transition_upload_pending(Pid, SID, BoundJID, User, MeshID,
                          Ready, Pending, Now) ->
    case {maps:get(Pid, Ready, none),
          maps:get(Pid, Pending, undefined)} of
        {{SID, User, MeshID}, {SID, BoundJID, Existing}} ->
            Current = purge_upload_ids(Existing, Now),
            update_upload_pending(Pid, SID, BoundJID, Current, Pending);
        _ -> maps:remove(Pid, Pending)
    end.

upload_delivery_current(Host, Packet,
                        {Pid, SID,
                         #jid{luser = User, lresource = MeshID} = BoundJID},
                        Ready) ->
    safe_upload_response_for_session(
      Packet, {Pid, SID, BoundJID}, Host, Ready, fun current_session_pid/3)
        andalso member_authorized(Host, MeshID, {User, Host});
upload_delivery_current(_Host, _Packet, _RecipientEpoch, _Ready) -> false.

safe_upload_response_for_session(Packet, {Pid, SID, BoundJID}, Host,
                                 Ready, SessionPid)
  when is_function(SessionPid, 3) ->
    case {xmpp:get_meta(Packet, ?UPLOAD_META, undefined),
          valid_upload_response_iq(Packet), xmpp:get_to(Packet),
          xmpp:get_from(Packet)} of
        {{upload_response, Pid, SID, BoundJID, ID}, {ok, ID},
         To, From} ->
            exact_jid(To, BoundJID)
                andalso exact_upload_domain(From, Host)
                andalso maps:get(Pid, Ready, none)
                    =:= {SID, BoundJID#jid.luser, BoundJID#jid.lresource}
                andalso safe_session_pid(SessionPid, BoundJID#jid.luser, Host,
                                         BoundJID#jid.lresource) =:= Pid;
        _ -> false
    end;
safe_upload_response_for_session(_Packet, _Epoch, _Host, _Ready,
                                 _SessionPid) -> false.

valid_upload_request_iq(#iq{id = ID, type = get, sub_els = [Element]}) ->
    case {valid_upload_id(ID), upload_xmlel(Element)} of
        {{ok, Random}, {ok, XML}} ->
            case valid_upload_request_element(XML, Random) of
                {ok, Size} -> {ok, ID, Size};
                error -> error
            end;
        _ -> error
    end;
valid_upload_request_iq(_) -> error.

valid_upload_response_iq(#iq{id = ID, type = result,
                             sub_els = [Element]}) ->
    case {valid_upload_id(ID), upload_xmlel(Element)} of
        {{ok, _}, {ok, XML}} ->
            case valid_upload_slot_element(XML) of
                true -> {ok, ID};
                false -> error
            end;
        _ -> error
    end;
valid_upload_response_iq(#iq{id = ID, type = error,
                             sub_els = [Request, #stanza_error{}]}) ->
    case {valid_upload_id(ID), upload_xmlel(Request)} of
        {{ok, Random}, {ok, XML}} ->
            case valid_upload_request_element(XML, Random) of
                {ok, _Size} -> {ok, ID};
                error -> error
            end;
        _ -> error
    end;
valid_upload_response_iq(_) -> error.

valid_upload_id(<<"cynapsa-upload-",
                  Random:?UPLOAD_RANDOM_BYTES/binary>> = ID)
  when byte_size(ID) =< ?MAX_UPLOAD_ID_BYTES ->
    case upload_random(Random) of true -> {ok, Random}; false -> error end;
valid_upload_id(_) -> error.

upload_random(Random) when byte_size(Random) =:= ?UPLOAD_RANDOM_BYTES ->
    lists:all(fun(Byte) ->
                      (Byte >= $A andalso Byte =< $Z)
                          orelse (Byte >= $2 andalso Byte =< $7)
              end, binary_to_list(Random));
upload_random(_) -> false.

upload_xmlel(#xmlel{} = Element) -> {ok, Element};
upload_xmlel(Element) ->
    try xmpp:encode(Element) of
        #xmlel{} = XML -> {ok, XML};
        _ -> error
    catch _:_ -> error end.

valid_upload_request_element(
  #xmlel{name = <<"request">>, attrs = Attrs, children = []}, Random) ->
    case exact_attrs(Attrs, [<<"xmlns">>, <<"filename">>, <<"size">>,
                             <<"content-type">>]) of
        {ok, Values} ->
            Filename = maps:get(<<"filename">>, Values),
            ContentType = maps:get(<<"content-type">>, Values),
            case {maps:get(<<"xmlns">>, Values),
                  valid_upload_filename(Filename, Random),
                  parse_decimal(maps:get(<<"size">>, Values),
                                ?MAX_UPLOAD_BYTES),
                  valid_upload_text(ContentType,
                                    ?MAX_UPLOAD_CONTENT_TYPE_BYTES)} of
                {?UPLOAD_NAMESPACE, true, {ok, Size}, true} when Size > 0 ->
                    {ok, Size};
                _ -> error
            end;
        error -> error
    end;
valid_upload_request_element(_, _) -> error.

valid_upload_filename(<<"cynapsa-",
                        Random:?UPLOAD_RANDOM_BYTES/binary,
                        ".bin">> = Filename, Random) ->
    byte_size(Filename) =< ?MAX_UPLOAD_FILENAME_BYTES;
valid_upload_filename(_, _) -> false.

valid_upload_slot_element(
  #xmlel{name = <<"slot">>, attrs = Attrs, children = Children}) ->
    case exact_attrs(Attrs, [<<"xmlns">>]) of
        {ok, #{<<"xmlns">> := ?UPLOAD_NAMESPACE}} ->
            valid_upload_slot_children(Children);
        _ -> false
    end;
valid_upload_slot_element(_) -> false.

valid_upload_slot_children([First, Second]) ->
    (valid_upload_put(First) andalso valid_upload_get(Second))
        orelse (valid_upload_get(First) andalso valid_upload_put(Second));
valid_upload_slot_children(_) -> false.

valid_upload_put(#xmlel{name = <<"put">>, attrs = Attrs,
                        children = Headers}) ->
    case exact_attrs(Attrs, [<<"url">>]) of
        {ok, #{<<"url">> := URL}} ->
            valid_upload_url(URL)
                andalso length(Headers) =< ?MAX_UPLOAD_HEADERS
                andalso valid_upload_headers(Headers, #{});
        _ -> false
    end;
valid_upload_put(_) -> false.

valid_upload_get(#xmlel{name = <<"get">>, attrs = Attrs, children = []}) ->
    case exact_attrs(Attrs, [<<"url">>]) of
        {ok, #{<<"url">> := URL}} ->
            valid_upload_url(URL);
        _ -> false
    end;
valid_upload_get(_) -> false.

valid_upload_headers([], _Seen) -> true;
valid_upload_headers([#xmlel{name = <<"header">>, attrs = Attrs,
                             children = [{xmlcdata, Value}]} | Rest], Seen) ->
    case exact_attrs(Attrs, [<<"name">>]) of
        {ok, #{<<"name">> := Name}} ->
            Lower = string:lowercase(Name),
            case valid_upload_text(Name, ?MAX_UPLOAD_HEADER_NAME_BYTES)
                 andalso allowed_upload_header(Lower)
                 andalso valid_upload_text(Value,
                                           ?MAX_UPLOAD_HEADER_VALUE_BYTES)
                 andalso not maps:is_key(Lower, Seen) of
                true -> valid_upload_headers(Rest, Seen#{Lower => true});
                false -> false
            end;
        _ -> false
    end;
valid_upload_headers(_, _) -> false.

allowed_upload_header(<<"authorization">>) -> true;
allowed_upload_header(<<"cookie">>) -> true;
allowed_upload_header(<<"expires">>) -> true;
allowed_upload_header(_) -> false.

valid_upload_url(<<"https://", _/binary>> = URL) ->
    valid_upload_text(URL, ?MAX_UPLOAD_URL_BYTES);
valid_upload_url(_) -> false.

valid_upload_text(Value, Maximum)
  when is_binary(Value), byte_size(Value) > 0, byte_size(Value) =< Maximum ->
    try unicode:characters_to_list(Value, utf8) of
        Characters when is_list(Characters) ->
            lists:all(fun(Character) ->
                              Character >= 16#20 andalso Character =/= 16#7f
                      end, Characters);
        _ -> false
    catch _:_ -> false end;
valid_upload_text(_, _) -> false.

bind_authorized(User, Host, Resource) ->
    case {canonical_user(User), canonical_host(Host), canonical_mesh_id(Resource)} of
        {{ok, LUser}, {ok, LHost}, {ok, MeshID}} ->
            authority_ready(LHost)
                andalso member_authorized(LHost, MeshID, {LUser, LHost});
        _ -> false
    end.

%%%===================================================================
%%% Authenticated, paginated authoritative-group IQ
%%%===================================================================

%% gen_iq_handler delegates the private namespace decoder to this module.
%% Keep the raw element; process_group_iq applies the complete strict schema.
decode_iq_subel(#xmlel{} = Element) -> Element;
decode_iq_subel(_) -> erlang:error({xmpp_codec, invalid_group_query}).

process_group_iq(#iq{type = get, from = From, to = To, lang = Lang,
                     sub_els = [#xmlel{} = Query]} = IQ) ->
    case parse_authority_sync(Query) of
        {ok, Nonce, Cursor} ->
            process_authority_sync(IQ, From, To, Lang, Nonce, Cursor);
        error -> iq_error(IQ, bad_request, Lang)
    end;
process_group_iq(#iq{lang = Lang} = IQ) ->
    iq_error(IQ, bad_request, Lang).

process_authority_sync(IQ,
                       #jid{luser = User, lserver = Host,
                            lresource = Resource} = From,
                       #jid{lserver = Host} = To, Lang, Nonce, Cursor) ->
    case {canonical_full_jid(From), exact_authority_domain(To, Host),
          canonical_user(User), canonical_mesh_id(Resource),
          bind_authorized(User, Host, Resource)} of
        {true, true, {ok, User}, {ok, _MeshID}, true} ->
            Origin = xmpp:get_meta(IQ, ?SESSION_META, undefined),
            case safe_call(Host, {authority_sync, From, Origin, Nonce,
                                  Cursor}) of
                {ok, terminal, Total, Cursor, Results,
                 Pid, SID, User, Resource} ->
                    Result = authority_sync_element(Nonce, Cursor, Total,
                                                    Results),
                    case byte_size(fxml:element_to_binary(Result)) =< ?MAX_PAGE_BYTES of
                        true ->
                            xmpp:put_meta(xmpp:make_iq_result(IQ, Result),
                                          ?SYNC_META,
                                          {Pid, SID, User, Resource});
                        false -> iq_error(IQ, resource_constraint, Lang)
                    end;
                {ok, Total, Cursor, Results} ->
                    Result = authority_sync_element(Nonce, Cursor, Total,
                                                    Results),
                    case byte_size(fxml:element_to_binary(Result)) =< ?MAX_PAGE_BYTES of
                        true -> xmpp:make_iq_result(IQ, Result);
                        false -> iq_error(IQ, resource_constraint, Lang)
                    end;
                {error, snapshot_conflict} ->
                    iq_error(IQ, conflict, Lang);
                {error, authority_capacity} ->
                    iq_error(IQ, resource_constraint, Lang);
                _ -> iq_error(IQ, service_unavailable, Lang)
            end;
        _ -> iq_error(IQ, forbidden, Lang)
    end;
process_authority_sync(IQ, _From, _To, Lang, _Nonce, _Cursor) ->
    iq_error(IQ, forbidden, Lang).

authority_sync_owned(Host, #jid{luser = User, lserver = Host,
                                lresource = MeshID} = From,
                     {Pid, SID, From}, Nonce, Cursor, Pending,
                     PendingMembers, PendingBytes, Now)
  when is_pid(Pid) ->
    CurrentPid = ejabberd_sm:get_session_pid(User, Host, MeshID),
    case CurrentPid =:= Pid andalso bind_authorized(User, Host, MeshID) of
        false -> {error, authority_unavailable};
        true -> case authority_snapshot_page(Host, MeshID, Pid, SID, User,
                                             Nonce, Cursor, Pending,
                                             PendingMembers, PendingBytes,
                                             Now) of
                    {ok, Members, Results, UpdatedPending} ->
                        %% Recheck exact-session ownership after the Mnesia
                        %% read; replacement cannot inherit readiness.
                        case ejabberd_sm:get_session_pid(User, Host, MeshID) of
                            Pid -> {ok, {Pid, SID}, User, MeshID, Members,
                                       Results, UpdatedPending};
                            _ -> {error, authority_unavailable}
                        end;
                    Error -> Error
                end
    end;
authority_sync_owned(_Host, _From, _Origin, _Nonce, _Cursor, _Pending,
                     _PendingMembers, _PendingBytes, _Now) ->
    {error, authority_unavailable}.

authority_snapshot_page(Host, MeshID, Pid, SID, User, Nonce, 0, Pending,
                        PendingMembers, PendingBytes, Now) ->
    case maps:get(Pid, Pending, undefined) of
        {_SID, _User, _MeshID, _Nonce, _Members, _Cursor, _Bytes, Expiry}
          when is_integer(Expiry), Expiry =< Now ->
            {error, snapshot_expired, Pid, User, MeshID};
        undefined ->
            authority_snapshot_first_page(Host, MeshID, Pid, SID, User,
                                          Nonce, Pending, PendingMembers,
                                          PendingBytes, Now);
        _ -> {error, snapshot_conflict}
    end;
authority_snapshot_page(_Host, MeshID, Pid, SID, User, Nonce, Cursor,
                        Pending, _PendingMembers, _PendingBytes, Now) ->
    case maps:get(Pid, Pending, undefined) of
        {SID, User, MeshID, Nonce, _Members, _Expected, _Bytes, Expiry}
          when is_integer(Expiry), Expiry =< Now ->
            {error, snapshot_expired, Pid, User, MeshID};
        {SID, User, MeshID, Nonce, Members, Cursor, Bytes, Expiry} ->
            Results = snapshot_page_members(Members, Cursor),
            Next = Cursor + length(Results),
            Updated = case Next < length(Members) of
                          true -> Pending#{Pid => {SID, User, MeshID, Nonce,
                                                   Members, Next, Bytes,
                                                   Expiry}};
                          false -> maps:remove(Pid, Pending)
                      end,
            {ok, Members, Results, Updated};
        _ -> {error, snapshot_conflict}
    end.

authority_snapshot_first_page(Host, MeshID, Pid, SID, User, Nonce, Pending,
                              PendingMembers, PendingBytes, Now) ->
    case pending_snapshot_count_available(map_size(Pending)) of
        true -> authority_snapshot_first_page_available(
                  Host, MeshID, Pid, SID, User, Nonce, Pending,
                  PendingMembers, PendingBytes, Now);
        false -> {error, authority_capacity}
    end.

authority_snapshot_first_page_available(
  Host, MeshID, Pid, SID, User, Nonce, Pending,
  PendingMembers, PendingBytes, Now) ->
    case read_snapshot(Host, MeshID) of
        {ok, BareMembers} ->
            Members = [jid:make(MemberUser, Host, MeshID)
                       || Member <- BareMembers,
                          #jid{luser = MemberUser} <- [jid:decode(Member)]],
            Results = snapshot_page_members(Members, 0),
            Next = length(Results),
            case Next < length(Members) of
                true ->
                    Bytes = snapshot_members_bytes(Members),
                    case pending_snapshot_capacity(map_size(Pending),
                                                   PendingMembers,
                                                   PendingBytes,
                                                   length(Members), Bytes) of
                        true ->
                            Expiry = Now + ?SNAPSHOT_PENDING_MILLISECONDS,
                            Updated = Pending#{Pid =>
                                              {SID, User, MeshID, Nonce,
                                               Members, Next, Bytes, Expiry}},
                            {ok, Members, Results, Updated};
                        false -> {error, authority_capacity}
                    end;
                false ->
                    {ok, Members, Results, maps:remove(Pid, Pending)}
            end;
        Error -> Error
    end.

pending_snapshot_count_available(PendingCount) ->
    PendingCount < ?MAX_PENDING_SYNCS.

snapshot_page_members(Members, Cursor) ->
    lists:sublist(lists:nthtail(Cursor, Members), ?MAX_PAGE_MEMBERS).

snapshot_members_bytes(Members) ->
    lists:sum([byte_size(jid:encode(Member)) || Member <- Members]).

pending_snapshot_capacity(PendingCount, PendingMembers, PendingBytes,
                          NewMembers, NewBytes) ->
    pending_snapshot_count_available(PendingCount)
        andalso PendingMembers + NewMembers =< ?MAX_PENDING_SNAPSHOT_MEMBERS
        andalso PendingBytes + NewBytes =< ?MAX_PENDING_SNAPSHOT_BYTES.

update_pending_state(Pid, UpdatedPending,
                     #state{pending_sync = Existing,
                            pending_sync_members = Members0,
                            pending_sync_bytes = Bytes0} = State) ->
    {OldMembers, OldBytes} = pending_entry_usage(
                               maps:get(Pid, Existing, undefined)),
    {NewMembers, NewBytes} = pending_entry_usage(
                               maps:get(Pid, UpdatedPending, undefined)),
    State#state{pending_sync = UpdatedPending,
                pending_sync_members = Members0 - OldMembers + NewMembers,
                pending_sync_bytes = Bytes0 - OldBytes + NewBytes}.

remove_pending_pid(Pid, #state{pending_sync = Pending} = State) ->
    update_pending_state(Pid, maps:remove(Pid, Pending), State).

pending_entry_usage({_SID, _User, _MeshID, _Nonce, Members, _Cursor, Bytes,
                     _Expiry}) when is_list(Members), is_integer(Bytes) ->
    {length(Members), Bytes};
pending_entry_usage(_) -> {0, 0}.

expire_pending_snapshots(Now,
                         #state{host = Host, pending_sync = Pending} = State) ->
    maps:fold(
      fun(Pid, {_SID, User, MeshID, _Nonce, _Members, _Cursor, _Bytes,
                Expiry}, Acc) when is_integer(Expiry), Expiry =< Now ->
              close_expired_snapshot(Pid, Host, User, MeshID),
              remove_pending_pid(Pid, Acc);
         (_Pid, _Entry, Acc) -> Acc
      end, State, Pending).

close_expired_snapshot(Pid, Host, User, MeshID) ->
    case current_session_pid(User, Host, MeshID) of
        Pid ->
            _ = catch ejabberd_c2s:close(Pid, snapshot_timeout),
            ok;
        _ -> ok
    end.

route_ready(Host, Packet,
                  #jid{luser = FromUser, lserver = Host,
                       lresource = MeshID},
                  #jid{luser = ToUser, lserver = Host,
                       lresource = MeshID}, Ready) ->
    FromPid = ejabberd_sm:get_session_pid(FromUser, Host, MeshID),
    Origin = xmpp:get_meta(Packet, ?SESSION_META, undefined),
    case {FromPid, Origin} of
        {none, _} -> false;
        {_, {FromPid, FromSID, _}} ->
            maps:get(FromPid, Ready, none)
                =:= {FromSID, FromUser, MeshID}
                andalso member_authorized(Host, MeshID, {FromUser, Host})
                andalso member_authorized(Host, MeshID, {ToUser, Host})
                andalso recipient_route_ready(Packet, ToUser, Host, MeshID,
                                               Ready);
        _ -> false
    end;
route_ready(_Host, _Packet, _From, _To, _Ready) -> false.

route_sender_ready(Host, Packet,
                   #jid{luser = FromUser, lserver = Host,
                        lresource = MeshID},
                   #jid{luser = ToUser, lserver = Host,
                        lresource = MeshID}, Ready) ->
    FromPid = ejabberd_sm:get_session_pid(FromUser, Host, MeshID),
    Origin = xmpp:get_meta(Packet, ?SESSION_META, undefined),
    case {FromPid, Origin} of
        {none, _} -> false;
        {_, {FromPid, FromSID, _}} ->
            maps:get(FromPid, Ready, none)
                =:= {FromSID, FromUser, MeshID}
                andalso member_authorized(Host, MeshID, {FromUser, Host})
                andalso member_authorized(Host, MeshID, {ToUser, Host});
        _ -> false
    end;
route_sender_ready(_Host, _Packet, _From, _To, _Ready) -> false.

recipient_route_ready(Packet, ToUser, Host, MeshID, Ready)
  when is_record(Packet, iq); is_record(Packet, presence) ->
    case ejabberd_sm:get_session_pid(ToUser, Host, MeshID) of
        none -> false;
        Pid -> case maps:get(Pid, Ready, none) of
                   {_SID, ToUser, MeshID} -> true;
                   _ -> false
               end
    end;
recipient_route_ready(#message{}, ToUser, Host, MeshID, Ready) ->
    case ejabberd_sm:get_session_pid(ToUser, Host, MeshID) of
        none -> false;
        Pid -> case maps:get(Pid, Ready, none) of
                   {_SID, ToUser, MeshID} -> true;
                   _ -> false
               end
    end;
recipient_route_ready(_, _ToUser, _Host, _MeshID, _Ready) ->
    false.

delivery_ready(Host, Packet, {Pid, SID,
                                    #jid{luser = User,
                                         lserver = Host,
                                         lresource = MeshID}}, Ready) ->
    #jid{luser = FromUser, lserver = FromHost, lresource = FromResource} =
        xmpp:get_from(Packet),
    FromHost =:= Host andalso FromResource =:= MeshID
        andalso member_authorized(Host, MeshID, {FromUser, Host})
        andalso member_authorized(Host, MeshID, {User, Host})
        andalso exact_session_epoch_current(Pid, SID, User, Host, MeshID,
                                            Ready,
                                            fun current_session_pid/3);
delivery_ready(_Host, _Packet, _RecipientEpoch, _Ready) -> false.

exact_session_epoch_current(Pid, SID, User, Host, MeshID, Ready, SessionPid) ->
    maps:get(Pid, Ready, none) =:= {SID, User, MeshID}
        andalso safe_session_pid(SessionPid, User, Host, MeshID) =:= Pid.

prepare_resume_transfer(OldPid, OldSID, NewPid, User, Host, MeshID, Ready,
                        SessionPid)
  when is_pid(OldPid), is_pid(NewPid), OldPid =/= NewPid ->
    case exact_session_epoch_current(OldPid, OldSID, User, Host, MeshID,
                                     Ready, SessionPid) of
        true -> {ok, make_ref()};
        false -> not_ready
    end;
prepare_resume_transfer(_OldPid, _OldSID, _NewPid, _User, _Host, _MeshID,
                        _Ready, _SessionPid) ->
    not_ready.

complete_resume_transfer(NewPid, NewSID, User, Host, MeshID, Token, Transfers,
                         SessionPid) ->
    case maps:get(NewPid, Transfers, none) of
        {Token, _OldPid, _OldSID, User, MeshID, Status}
          when Status =:= ready; Status =:= not_ready ->
            case safe_session_pid(SessionPid, User, Host, MeshID) of
                NewPid when NewSID =/= undefined -> {ok, Status};
                _ -> error
            end;
        _ -> error
    end.

ensure_session_monitor(Pid, Monitors) ->
    case maps:is_key(Pid, Monitors) of
        true -> Monitors;
        false -> Monitors#{Pid => erlang:monitor(process, Pid)}
    end.

authority_sync_element(Nonce, Cursor, Total, Results) ->
    NextIndex = Cursor + length(Results),
    Next = case NextIndex < Total of
               true -> integer_to_binary(NextIndex);
               false -> <<>>
           end,
    #xmlel{name = <<"synchronized">>,
           attrs = [{<<"xmlns">>, ?AUTHORITY_NAMESPACE},
                    {<<"nonce">>, Nonce},
                    {<<"total">>, integer_to_binary(Total)},
                    {<<"cursor">>, integer_to_binary(Cursor)},
                    {<<"next">>, Next}],
           children = [#xmlel{name = <<"member">>,
                              attrs = [{<<"jid">>, jid:encode(Peer)}],
                              children = []}
                       || Peer <- Results]}.

parse_authority_sync(#xmlel{name = <<"sync">>, attrs = Attrs,
                            children = []}) ->
    case exact_attrs(Attrs, [<<"xmlns">>, <<"nonce">>, <<"cursor">>]) of
        {ok, Values} ->
            case {maps:get(<<"xmlns">>, Values),
                  valid_nonce(maps:get(<<"nonce">>, Values)),
                  parse_decimal(maps:get(<<"cursor">>, Values), ?MAX_MEMBERS)} of
                {?AUTHORITY_NAMESPACE, true, {ok, Cursor}} ->
                    {ok, maps:get(<<"nonce">>, Values), Cursor};
                _ -> error
            end;
        error -> error
    end;
parse_authority_sync(_) -> error.

valid_nonce(Value) when is_binary(Value), byte_size(Value) >= 16,
                         byte_size(Value) =< 64 ->
    lists:all(fun(Byte) ->
                      (Byte >= $a andalso Byte =< $z) orelse
                      (Byte >= $A andalso Byte =< $Z) orelse
                      (Byte >= $0 andalso Byte =< $9) orelse Byte =:= $- orelse
                      Byte =:= $_
              end, binary_to_list(Value));
valid_nonce(_) -> false.

exact_attrs(Attrs, Names) when length(Attrs) =:= length(Names) ->
    exact_attrs(Attrs, Names, #{});
exact_attrs(_, _) -> error.

exact_attrs([], [], Values) -> {ok, Values};
exact_attrs([{Name, Value} | Rest], Names, Values)
  when is_binary(Name), is_binary(Value) ->
    case lists:member(Name, Names) andalso not maps:is_key(Name, Values) of
        true -> exact_attrs(Rest, lists:delete(Name, Names),
                            Values#{Name => Value});
        false -> error
    end;
exact_attrs(_, _, _) -> error.

parse_decimal(<<"0">>, _Maximum) -> {ok, 0};
parse_decimal(<<First, _/binary>> = Value, Maximum)
  when First >= $1, First =< $9, byte_size(Value) =< 20 ->
    case all_decimal(Value) of
        true ->
            try binary_to_integer(Value) of
                Number when Number =< Maximum -> {ok, Number};
                _ -> error
            catch _:_ -> error end;
        false -> error
    end;
parse_decimal(_, _) -> error.

all_decimal(Value) ->
    lists:all(fun(Byte) -> Byte >= $0 andalso Byte =< $9 end,
              binary_to_list(Value)).

iq_error(IQ, bad_request, Lang) ->
    xmpp:make_error(IQ, xmpp:err_bad_request(<<"Invalid Cynapsa mesh query">>, Lang));
iq_error(IQ, forbidden, Lang) ->
    xmpp:make_error(IQ, xmpp:err_forbidden(<<"Cynapsa mesh query is not authorized">>, Lang));
iq_error(IQ, conflict, Lang) ->
    xmpp:make_error(IQ, xmpp:err_conflict(<<"Cynapsa mesh snapshot changed">>, Lang));
iq_error(IQ, resource_constraint, Lang) ->
    xmpp:make_error(IQ, xmpp:err_resource_constraint(<<"Cynapsa mesh page exceeds limits">>, Lang));
iq_error(IQ, service_unavailable, Lang) ->
    xmpp:make_error(IQ, xmpp:err_service_unavailable(<<"Cynapsa mesh authority unavailable">>, Lang)).

member_authorized(Host, MeshID, US) ->
    Group = internal_group(MeshID),
    mesh_exists(Group, Host)
        andalso mod_shared_roster:is_user_in_group(US, Group, Host).

%%%===================================================================
%%% Restricted commands
%%%===================================================================

cynapsa_mesh_ready(Host0) ->
    case canonical_host(Host0) of
        {ok, Host} ->
            case safe_call(Host, verify_authority) of
                ok -> {<<"ready">>, Host, <<"single_node_mnesia">>};
                _ -> {<<"not_ready">>, Host, <<"fail_closed">>}
            end;
        error -> {<<"not_ready">>, <<>>, <<"invalid_host">>}
    end.

cynapsa_mesh_create(MeshID, Host0) ->
    command_call(Host0, {create, MeshID}).

cynapsa_mesh_add(User, Host0, MeshID) ->
    command_call(Host0, {add, User, MeshID}).

cynapsa_mesh_remove(User, Host0, MeshID) ->
    command_call(Host0, {remove, User, MeshID}).

cynapsa_mesh_remove_many(UsersCSV, Host0, MeshID) ->
    command_call(Host0, {remove_many, binary:split(UsersCSV, <<",">>, [global]),
                         MeshID}).

cynapsa_mesh_snapshot(MeshID, Host0) ->
    case command_call(Host0, {snapshot, MeshID}) of
        {ok, Members} -> Members;
        _ -> []
    end.

get_commands_spec() ->
    [#ejabberd_commands{name = cynapsa_mesh_ready,
                        tags = [cynapsa, mesh],
                        desc = "Check the single-node Cynapsa mesh authority",
                        module = ?MODULE, function = cynapsa_mesh_ready,
                        args = [{host, binary}],
                        result = {ready, {tuple, [{status, string},
                                                  {host, string},
                                                  {authority, string}]}}},
     #ejabberd_commands{name = cynapsa_mesh_create,
                        tags = [cynapsa, mesh],
                        desc = "Create an authoritative Cynapsa mesh",
                        module = ?MODULE, function = cynapsa_mesh_create,
                        args = [{mesh, binary}, {host, binary}],
                        result = {res, rescode}},
     #ejabberd_commands{name = cynapsa_mesh_add,
                        tags = [cynapsa, mesh],
                        desc = "Add a bare local JID to a Cynapsa mesh",
                        module = ?MODULE, function = cynapsa_mesh_add,
                        args = [{user, binary}, {host, binary}, {mesh, binary}],
                        result = {res, rescode}},
     #ejabberd_commands{name = cynapsa_mesh_remove,
                        tags = [cynapsa, mesh],
                        desc = "Remove and disconnect a Cynapsa mesh member",
                        module = ?MODULE, function = cynapsa_mesh_remove,
                        args = [{user, binary}, {host, binary}, {mesh, binary}],
                        result = {res, rescode}},
     #ejabberd_commands{name = cynapsa_mesh_remove_many,
                        tags = [cynapsa, mesh],
                        desc = "Remove comma-separated members atomically",
                        module = ?MODULE, function = cynapsa_mesh_remove_many,
                        args = [{users, binary}, {host, binary}, {mesh, binary}],
                        result = {res, rescode}},
     #ejabberd_commands{name = cynapsa_mesh_snapshot,
                        tags = [cynapsa, mesh],
                        desc = "Read a consistent Cynapsa mesh snapshot",
                        module = ?MODULE, function = cynapsa_mesh_snapshot,
                        args = [{mesh, binary}, {host, binary}],
                        result = {members, {list, {member, string}}}}].

command_call(Host0, Request) ->
    case canonical_host(Host0) of
        {ok, Host} ->
            case safe_call(Host, Request) of
                ok -> ok;
                {ok, _} = Result -> Result;
                {ok, _, _} = Result -> Result;
                {error, Reason} -> Reason;
                _ -> authority_unavailable
            end;
        error -> invalid_host
    end.

safe_call(Host, Request) ->
    try gen_server:call(proc(Host), Request, ?CALL_TIMEOUT) of
        Reply -> Reply
    catch
        exit:_ -> {error, authority_unavailable}
    end.

mutation_reply({ok, changed, MeshID, Members, Removed}, _RequestedRemoved,
               #state{host = Host} = State) ->
    %% The durable membership commit happened in the serialized call above.
    %% Fence all exact sessions and pending resume handoffs before any purge,
    %% kick, or live notification. Retained c2s streams and their indivisible
    %% XEP-0198 FIFOs remain intact so snapshot-required can replay after a
    %% transport outage. Only removed exact resources are closed.
    Retained = retained_ready_sessions(Members, Host, MeshID,
                                       State#state.ready),
    Updated0 = invalidate_mesh(MeshID, State),
    Purge = purge_removed_state(Removed, Host, MeshID),
    Revoke = revoke_exact_sessions(Removed, Host, MeshID),
    Updated = notify_membership_sessions(Retained, Host, Updated0),
    Reply = cleanup_reply([Purge, Revoke]),
    {reply, Reply, Updated};
mutation_reply({ok, cleanup, MeshID, Removed}, _RequestedRemoved,
               #state{host = Host} = State) ->
    %% A prior command may have committed membership removal but timed out or
    %% failed during cleanup. Idempotent retry must repeat the purge and exact
    %% close instead of reporting success merely because membership is absent.
    Purge = purge_removed_state(Removed, Host, MeshID),
    Revoke = revoke_exact_sessions(Removed, Host, MeshID),
    Reply = cleanup_reply([Purge, Revoke]),
    {reply, Reply, State};
mutation_reply(ok, _Removed, State) -> {reply, ok, State};
mutation_reply(Error, _Removed, State) -> {reply, Error, State}.

cleanup_reply([]) -> ok;
cleanup_reply([ok | Rest]) -> cleanup_reply(Rest);
cleanup_reply([{error, Reason} | _]) -> {error, Reason};
cleanup_reply([_ | _]) -> {error, membership_cleanup_failed}.

invalidate_mesh(MeshID,
                #state{ready = Ready, pending_sync = Pending,
                       terminal_sync = Terminal,
                       resume_transfers = Transfers,
                       upload_pending = UploadPending} = State) ->
    KeepReady = maps:filter(fun(_Pid, {_SID, _User, EntryMesh}) ->
                                    EntryMesh =/= MeshID
                            end, Ready),
    KeepPending = maps:filter(
                    fun(_Pid, {_SID, _User, EntryMesh, _Nonce,
                               _Members, _Cursor, _Bytes, _Expiry}) ->
                            EntryMesh =/= MeshID;
                       (_Pid, _Invalid) -> false
                    end, Pending),
    KeepUploads = maps:filter(
                    fun(_Pid, {_SID, #jid{lresource = EntryMesh}, _IDs}) ->
                            EntryMesh =/= MeshID;
                       (_Pid, _Invalid) -> false
                    end, UploadPending),
    KeepTerminal = maps:filter(
                     fun(_Pid, {_SID, _User, EntryMesh}) ->
                             EntryMesh =/= MeshID;
                        (_Pid, _Invalid) -> false
                     end, Terminal),
    KeepTransfers = maps:filter(
                      fun(_Pid, {_Token, _OldPid, _OldSID, _User,
                                 EntryMesh, _Status}) ->
                              EntryMesh =/= MeshID;
                         (_Pid, _Invalid) -> false
                      end, Transfers),
    PendingState = replace_all_pending(KeepPending, State),
    PendingState#state{ready = KeepReady,
                       terminal_sync = KeepTerminal,
                       resume_transfers = KeepTransfers,
                       upload_pending = KeepUploads}.

retained_ready_sessions(Members, Host, MeshID, Ready) ->
    Current = lists:usort(Members),
    maps:fold(
      fun(Pid, {SID, User, EntryMesh}, Acc) when EntryMesh =:= MeshID ->
              case lists:member({User, Host}, Current) of
                  true -> [{Pid, SID, User, MeshID} | Acc];
                  false -> Acc
              end;
         (_Pid, _Invalid, Acc) -> Acc
      end, [], Ready).

replace_all_pending(Pending, State) ->
    {Members, Bytes} = maps:fold(
                         fun(_Pid, Entry, {MemberTotal, ByteTotal}) ->
                                 {EntryMembers, EntryBytes} =
                                     pending_entry_usage(Entry),
                                 {MemberTotal + EntryMembers,
                                  ByteTotal + EntryBytes}
                         end, {0, 0}, Pending),
    State#state{pending_sync = Pending, pending_sync_members = Members,
                pending_sync_bytes = Bytes}.

%%%===================================================================
%%% Authoritative Mnesia operations
%%%===================================================================

create_mesh(Host, MeshID0) ->
    case {validate_authority(Host), canonical_mesh_id(MeshID0)} of
        {ok, {ok, MeshID}} ->
            Group = internal_group(MeshID),
            transaction(fun() -> create_mesh_tx(Host, Group) end);
        {{error, Reason}, _} -> {error, Reason};
        {_, error} -> {error, invalid_mesh_id}
    end.

create_mesh_tx(Host, Group) ->
    Key = {Group, Host},
    case mnesia:read(sr_group, Key, write) of
        [] ->
            case count_meshes_tx(Host) < ?MAX_MESHES of
                true ->
                    Opts = [{label, Group}, {description, <<>>},
                            {displayed_groups, []}],
                    mnesia:write(#sr_group{group_host = Key, opts = Opts}),
                    ok;
                false -> {error, mesh_capacity}
            end;
        [_] -> ok
    end.

add_member(Host, User0, MeshID0) ->
    case canonical_inputs(Host, User0, MeshID0) of
        {ok, User, MeshID} ->
            Group = internal_group(MeshID),
            case member_authorized(Host, MeshID, {User, Host}) of
                true -> ok;
                false ->
                    case ejabberd_sm:get_session_pid(User, Host, MeshID) of
                        none ->
                            case transaction(fun() -> add_member_tx(Host, User, Group) end) of
                                {ok, added, Members} ->
                                    {ok, changed, MeshID, Members, []};
                                {ok, present} -> ok;
                                Error -> Error
                            end;
                        _ -> {error, session_revocation_pending}
                    end
            end;
        {error, Reason} -> {error, Reason}
    end.

add_member_tx(Host, User, Group) ->
    Key = {Group, Host},
    US = {User, Host},
    case mnesia:read(sr_group, Key, read) of
        [] -> {error, unknown_mesh};
        [_] ->
            Members = group_members_tx(Key),
            case lists:member(US, Members) of
                true -> {ok, present};
                false when length(Members) >= ?MAX_MEMBERS ->
                    {error, member_capacity};
                false ->
                    NewMembers = lists:sort([US | Members]),
                    mnesia:write(#sr_user{us = US, group_host = Key}),
                    {ok, added, NewMembers}
            end
    end.

remove_member(Host, Users0, MeshID0) when is_list(Users0) ->
    case canonical_remove_inputs(Host, Users0, MeshID0) of
        {ok, Users, MeshID} ->
            Group = internal_group(MeshID),
            case transaction(fun() ->
                                     remove_members_tx(Host, Users, Group,
                                                       MeshID)
                             end) of
                {ok, removed, Members, Removed} ->
                    {ok, changed, MeshID, Members, Removed};
                {ok, absent, Removed} ->
                    {ok, cleanup, MeshID, Removed};
                Error -> Error
            end;
        {error, Reason} -> {error, Reason}
    end.

remove_members_tx(Host, Users, Group, MeshID) ->
    Key = {Group, Host},
    case mnesia:read(sr_group, Key, read) of
        [] -> {error, unknown_mesh};
        [_] ->
            Members = group_members_tx(Key),
            Targets = lists:usort([{User, Host} || User <- Users]),
            Removed = [US || US <- Targets, lists:member(US, Members)],
            case Removed of
                [] -> {ok, absent, Targets};
                _ ->
                    lists:foreach(fun(US) ->
                                          mnesia:delete_object(
                                            #sr_user{us = US, group_host = Key})
                                  end, Removed),
                    %% Membership and its durable exact-resource rows share
                    %% one Mnesia commit.  A server crash can therefore never
                    %% leave rows that a later re-add would resurrect.
                    purge_removed_mailbox_resources_tx(
                      [{User, Host, MeshID}
                       || {User, MemberHost} <- Removed,
                          MemberHost =:= Host]),
                    {ok, removed, Members -- Removed, Removed}
            end
    end.

read_snapshot(Host, MeshID0) ->
    case canonical_mesh_id(MeshID0) of
        {ok, MeshID} ->
            Group = internal_group(MeshID),
            case transaction(fun() -> snapshot_tx(Host, Group) end) of
                {ok, Members} ->
                    Encoded = [jid:encode(jid:make(User, Server))
                               || {User, Server} <- Members],
                    {ok, Encoded};
                Error -> Error
            end;
        error -> {error, invalid_mesh_id}
    end.

snapshot_tx(Host, Group) ->
    Key = {Group, Host},
    case mnesia:read(sr_group, Key, read) of
        [_] ->
            Members = group_members_tx(Key),
            case length(Members) =< ?MAX_MEMBERS of
                true -> {ok, Members};
                false -> {error, member_capacity}
            end;
        _ -> {error, unknown_or_invalid_mesh}
    end.

transaction(Fun) ->
    case mnesia:transaction(Fun, [], 3) of
        {atomic, Result} -> Result;
        {aborted, _} -> {error, authority_storage_failure}
    end.

group_members_tx(Key) ->
    lists:sort([US || #sr_user{us = US} <-
                         mnesia:index_read(sr_user, Key, #sr_user.group_host)]).

mesh_exists(Group, Host) ->
    case mnesia:dirty_read(sr_group, {Group, Host}) of
        [_] -> true;
        _ -> false
    end.

%%%===================================================================
%%% Validation and authority invariants
%%%===================================================================

initialize_authority(Host) ->
    case validate_authority(Host) of
        ok ->
            case ensure_mailbox_tables() of
                ok ->
                    case ensure_upload_object_table() of
                        ok ->
                            case enforce_mam_never(Host) of
                                ok -> required_authority_tables_local();
                                Error -> Error
                            end;
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error -> Error
    end.

ensure_mailbox_tables() ->
    try
        Message = ejabberd_mnesia:create(
                    ?MODULE, cynapsa_mesh_mailbox,
                    [{disc_copies, [node()]},
                     {type, ordered_set},
                     {attributes, record_info(fields,
                                              cynapsa_mesh_mailbox)},
                     {index, [owner, sender, message_id]}]),
        Cursor = ejabberd_mnesia:create(
                   ?MODULE, cynapsa_mesh_mailbox_cursor,
                   [{disc_copies, [node()]},
                    {attributes, record_info(fields,
                                             cynapsa_mesh_mailbox_cursor)}]),
        case {Message, Cursor} of
            {{atomic, _}, {atomic, _}} -> ok;
            _ -> {error, mailbox_storage_failure}
        end
    catch _:_ -> {error, mailbox_storage_failure}
    end.

ensure_upload_object_table() ->
    try ejabberd_mnesia:create(
          ?MODULE, cynapsa_mesh_upload_object,
          [{disc_copies, [node()]},
           {attributes, record_info(fields, cynapsa_mesh_upload_object)}]) of
        {atomic, _} -> ok;
        _ -> {error, upload_ownership_storage_failure}
    catch _:_ -> {error, upload_ownership_storage_failure}
    end.

enforce_mam_never(Host) ->
    AuthorityHost = Host,
    Rewrite = fun() ->
                      mnesia:foldl(
                        fun(#archive_prefs{us = {_User, RowHost}} = Prefs, Acc)
                              when RowHost =:= AuthorityHost ->
                                mnesia:write(
                                  Prefs#archive_prefs{default = never,
                                                      always = []}),
                                Acc;
                           (#archive_prefs{}, Acc) -> Acc;
                           (_, _Acc) -> mnesia:abort(invalid_archive_prefs)
                        end, ok, archive_prefs)
              end,
    case mnesia:transaction(Rewrite, [], 3) of
        {atomic, ok} ->
            _ = catch ets_cache:clear(archive_prefs_cache),
            verify_mam_never(Host);
        {aborted, _} -> {error, mam_preference_migration_failed}
    end.

verify_mam_never(Host) ->
    AuthorityHost = Host,
    Verify = fun() ->
                     mnesia:foldl(
                       fun(#archive_prefs{us = {_User, RowHost},
                                          default = never,
                                          always = []}, Acc)
                             when RowHost =:= AuthorityHost -> Acc;
                          (#archive_prefs{us = {_User, RowHost}}, _Acc)
                             when RowHost =:= AuthorityHost ->
                               mnesia:abort(mam_archiving_enabled);
                          (#archive_prefs{}, Acc) -> Acc;
                          (_, _Acc) -> mnesia:abort(invalid_archive_prefs)
                       end, ok, archive_prefs)
             end,
    case mnesia:transaction(Verify, [], 3) of
        {atomic, ok} -> ok;
        {aborted, _} -> {error, mam_archiving_enabled}
    end.

required_authority_tables_local() ->
    Tables = [sr_group, sr_user, offline_msg, archive_msg, archive_prefs,
              cynapsa_mesh_upload_object, cynapsa_mesh_mailbox,
              cynapsa_mesh_mailbox_cursor],
    case lists:all(fun(Table) ->
                           try mnesia:table_info(Table, where_to_read) of
                               Node when Node =:= node() -> true;
                               _ -> false
                           catch _:_ -> false
                           end
                   end, Tables) of
        true -> ok;
        false -> {error, authority_table_unavailable}
    end.

validate_authority(Host) ->
    case canonical_host(Host) of
        {ok, Host} ->
            case mnesia:system_info(running_db_nodes) of
                [Node] when Node =:= node() ->
                    case gen_mod:is_loaded(Host, mod_shared_roster) of
                        true ->
                            case {gen_mod:db_mod(Host, mod_shared_roster),
                                  gen_mod:db_mod(Host, mod_offline),
                                  gen_mod:db_mod(Host, mod_mam),
                                  mod_mam_opt:assume_mam_usage(Host),
                                  mod_mam_opt:default(Host),
                                  mod_mam_opt:request_activates_archiving(Host),
                                  ejabberd_option:resource_conflict(Host)} of
                                {mod_shared_roster_mnesia,
                                 mod_offline_mnesia, mod_mam_mnesia,
                                 false, never, false, Conflict}
                                  when Conflict =/= setresource -> ok;
                                {mod_shared_roster_mnesia,
                                 mod_offline_mnesia, mod_mam_mnesia,
                                 false, never, false,
                                 setresource} ->
                                    {error, resource_substitution_unsupported};
                                _ -> {error, mesh_state_configuration_unsafe}
                            end;
                        false -> {error, shared_roster_not_loaded}
                    end;
                _ -> {error, clustered_authority_unsupported}
            end;
        error -> {error, invalid_host}
    end.

authority_ready(Host) ->
    case validate_authority(Host) of
        ok -> required_authority_tables_local() =:= ok;
        _ -> false
    end.

canonical_inputs(Host, User0, MeshID0) ->
    case {canonical_user(User0), canonical_host(Host),
          canonical_mesh_id(MeshID0)} of
        {{ok, User}, {ok, Host}, {ok, MeshID}} -> {ok, User, MeshID};
        {error, _, _} -> {error, invalid_user};
        {_, error, _} -> {error, invalid_host};
        {_, _, error} -> {error, invalid_mesh_id}
    end.

canonical_remove_inputs(Host, Users0, MeshID0)
  when is_list(Users0), length(Users0) > 0, length(Users0) =< ?MAX_MEMBERS ->
    case {canonical_host(Host), canonical_mesh_id(MeshID0),
          [canonical_user(User) || User <- Users0]} of
        {{ok, Host}, {ok, MeshID}, Canonical} ->
            case [User || {ok, User} <- Canonical] of
                Users when length(Users) =:= length(Canonical) ->
                    {ok, lists:usort(Users), MeshID};
                _ -> {error, invalid_user}
            end;
        {error, _, _} -> {error, invalid_host};
        {_, error, _} -> {error, invalid_mesh_id}
    end;
canonical_remove_inputs(_Host, _Users, _MeshID) -> {error, invalid_user}.

canonical_user(User) when is_binary(User), byte_size(User) > 0,
                          byte_size(User) =< ?MAX_USER_BYTES ->
    case jid:nodeprep(User) of
        User -> {ok, User};
        _ -> error
    end;
canonical_user(_) -> error.

canonical_host(Host) when is_binary(Host), byte_size(Host) > 0,
                          byte_size(Host) =< ?MAX_USER_BYTES ->
    case jid:nameprep(Host) of
        Host -> {ok, Host};
        _ -> error
    end;
canonical_host(_) -> error.

canonical_mesh_id(MeshID) when is_binary(MeshID), byte_size(MeshID) > 0,
                                byte_size(MeshID) =< ?MAX_MESH_ID_BYTES ->
    case {binary:match(MeshID, <<"/">>), jid:resourceprep(MeshID)} of
        {nomatch, MeshID} -> {ok, MeshID};
        _ -> error
    end;
canonical_mesh_id(_) -> error.

internal_group(MeshID) ->
    <<?GROUP_PREFIX/binary, MeshID/binary>>.

is_internal_group(<<"cynapsa_mesh_", MeshID/binary>>) ->
    canonical_mesh_id(MeshID) =/= error;
is_internal_group(_) -> false.

count_meshes_tx(Host) ->
    Groups = mnesia:select(
               sr_group,
               [{#sr_group{group_host = {'$1', Host}, _ = '_'},
                 [], ['$1']}]),
    length([Group || Group <- Groups, is_internal_group(Group)]).

revoke_exact_sessions(Members, Host, Resource) ->
    Users = lists:usort([User || {User, MemberHost} <- Members,
                                 MemberHost =:= Host]),
    Kick = lists:foldl(
             fun(User, Result) ->
                     case catch ejabberd_sm:kick_user(User, Host, Resource) of
                         {'EXIT', _} -> error;
                         _ -> Result
                     end
             end, ok, Users),
    case {Kick, await_sessions_closed(Users, Host, Resource, 50)} of
        {ok, ok} -> ok;
        {error, _} -> {error, session_revocation_failed};
        {_, {error, _}} -> {error, session_revocation_timeout}
    end.

notify_membership_sessions(Sessions, Host,
                           #state{pending_controls = Pending,
                                  control_pending_max = Maximum,
                                  control_ack_timeout_ms = Timeout} = State) ->
    Now = erlang:monotonic_time(millisecond),
    %% Router success is only an enqueue attempt, never delivery evidence.  A
    %% tracked exact-session IQ acknowledgement is required; every exception
    %% or non-ok result closes that exact control owner.
    Route = fun(Packet) -> ejabberd_router:route(Packet) end,
    Close = fun(Pid, Reason) ->
                    ejabberd_c2s:close(Pid, Reason),
                    ok
            end,
    Updated = queue_membership_controls(Sessions, Host, Pending, Now, Timeout,
                                         Maximum, Route, Close),
    State#state{pending_controls = Updated}.

queue_membership_controls(Sessions, Host, Pending, Now, Timeout, Maximum,
                          Route, Close)
  when is_list(Sessions), is_map(Pending), is_integer(Now),
       is_integer(Timeout), Timeout > 0, is_integer(Maximum), Maximum > 0,
       is_function(Route, 1), is_function(Close, 2) ->
    queue_membership_controls(Sessions, Host, Pending, Now, Timeout, Maximum,
                              Route, Close, fun current_session_pid/3).

queue_membership_controls(Sessions, Host, Pending, Now, Timeout, Maximum,
                          Route, Close, SessionPid)
  when is_list(Sessions), is_map(Pending), is_integer(Now),
       is_integer(Timeout), Timeout > 0, is_integer(Maximum), Maximum > 0,
       is_function(Route, 1), is_function(Close, 2),
       is_function(SessionPid, 3) ->
    lists:foldl(
      fun({Pid, SID, User, Resource}, Acc) ->
              case map_size(Acc) < Maximum
                   andalso not control_owner_exists(Pid, Acc) of
                  true -> queue_membership_control(
                            Pid, SID, User, Host, Resource, Acc, Now,
                            Timeout, Route, Close, SessionPid);
                  false ->
                      close_control_owner(Pid, authority_control_capacity,
                                          Close),
                      remove_control_owner(Pid, Acc)
              end
      end, Pending, Sessions).

queue_membership_control(Pid, SID, User, Host, Resource, Pending, Now,
                         Timeout, Route, Close, SessionPid) ->
    From = jid:make(<<>>, Host, <<>>),
    Control = #xmlel{name = <<"membership-changed">>,
                     attrs = [{<<"xmlns">>, ?AUTHORITY_NAMESPACE},
                              {<<"snapshot-required">>, <<"true">>}],
                     children = []},
    To = jid:make(User, Host, Resource),
    ID = <<?CONTROL_ID_PREFIX/binary,
           (integer_to_binary(erlang:unique_integer(
                                [positive, monotonic])))/binary>>,
    Packet = #iq{type = set, id = ID, from = From, to = To,
                 sub_els = [Control]},
    case exact_session_epoch_current(Pid, SID, User, Host, Resource,
                                     #{Pid => {SID, User, Resource}},
                                     SessionPid) of
        false ->
            close_control_owner(Pid, authority_control_session_replaced,
                                Close),
            remove_control_owner(Pid, Pending);
        true ->
            try Route(Packet) of
                ok -> Pending#{ID => {Pid, SID, To, Now + Timeout}};
                _ ->
                    close_control_owner(Pid, authority_control_enqueue_failed,
                                        Close),
                    Pending
            catch _:_ ->
                close_control_owner(Pid, authority_control_enqueue_failed,
                                    Close),
                Pending
            end
    end.

control_owner_exists(Pid, Pending) ->
    maps:fold(fun(_ID, {Owner, _SID, _BoundJID, _Expiry}, Found) ->
                      Found orelse Owner =:= Pid;
                 (_ID, _Invalid, _Found) -> true
              end, false, Pending).

remove_control_owner(Pid, Pending) ->
    maps:filter(
      fun(_ID, {Owner, _SID, _BoundJID, _Expiry}) -> Owner =/= Pid;
         (_ID, _Invalid) -> false
      end, Pending).

consume_control_ack(Pid, SID, #jid{} = BoundJID, ID, Pending) ->
    case maps:get(ID, Pending, undefined) of
        {Pid, SID, #jid{} = OwnerJID, _Expiry} ->
            case canonical_full_jid(BoundJID)
                 andalso exact_jid(BoundJID, OwnerJID) of
                true -> {ok, maps:remove(ID, Pending)};
                false -> error
            end;
        _ -> error
    end;
consume_control_ack(_Pid, _SID, _BoundJID, _ID, _Pending) -> error.

transfer_control_owner(Pending, OldPid, OldSID, NewPid, NewSID, BoundJID) ->
    maps:fold(
      fun(ID, {OwnerPid, OwnerSID, OwnerJID, Expiry}, Acc)
            when OwnerPid =:= OldPid, OwnerSID =:= OldSID,
                 OwnerJID =:= BoundJID ->
              Acc#{ID => {NewPid, NewSID, OwnerJID, Expiry}};
         (ID, Entry, Acc) -> Acc#{ID => Entry}
      end, #{}, Pending).

expire_control_acks(Now, #state{} = State) ->
    Close = fun(Pid, Reason) ->
                    ejabberd_c2s:close(Pid, Reason),
                    ok
            end,
    expire_control_acks(Now, State, Close).

expire_control_acks(Now, #state{pending_controls = Pending} = State, Close)
  when is_integer(Now), is_function(Close, 2) ->
    Updated = maps:fold(
                fun(ID, {Pid, _SID, _BoundJID, Expiry}, Acc)
                      when is_integer(Expiry), Expiry =< Now ->
                        close_control_owner(Pid,
                                            authority_control_ack_timeout,
                                            Close),
                        maps:remove(ID, Acc);
                   (_ID, _Entry, Acc) -> Acc
                end, Pending, Pending),
    State#state{pending_controls = Updated}.

close_control_owner(Pid, Reason, Close)
  when is_pid(Pid), is_function(Close, 2) ->
    try Close(Pid, Reason) catch _:_ -> error end.

close_exact_session(Pid, Reason) when is_pid(Pid) ->
    _ = catch ejabberd_c2s:close(Pid, Reason),
    ok;
close_exact_session(_Pid, _Reason) -> ok.

purge_removed_state([], _Host, _MeshID) -> ok;
purge_removed_state(Removed, Host, MeshID) ->
    Mailbox = purge_removed_mailbox(Removed, Host, MeshID),
    Offline = purge_table_if_present(
                offline_msg,
                fun({offline_msg, _US, _Timestamp, _Expire, From, To,
                     Packet}) ->
                        removed_mesh_packet(From, To, Removed, Host, MeshID)
                            orelse packet_removed_mesh(Packet, Removed, Host,
                                                       MeshID);
                   (_) -> false
                end),
    %% Direct selective Mnesia deletion cannot decrement mod_offline's cached
    %% per-user counters safely. Clear only that derived cache; the next count
    %% is reconstructed from authoritative records.
    _ = catch ets_cache:clear(offline_msg_counter_cache),
    Archive = purge_table_if_present(
                archive_msg,
                fun({archive_msg, {Owner, OwnerHost}, _ID, _Timestamp, Peer,
                     _BarePeer, Packet, _Nick, _Type, _OriginID})
                      when OwnerHost =:= Host ->
                        removed_archive(Owner, Peer, Packet, Removed,
                                        OwnerHost, MeshID);
                   (_) -> false
                end),
    Uploads = purge_removed_uploads(Removed, Host, MeshID),
    stored_cleanup_reply(Mailbox, Offline, Archive, Uploads).

stored_cleanup_reply(ok, ok, ok, ok) -> ok;
stored_cleanup_reply({error, _} = Error, _Offline, _Archive, _Uploads) ->
    Error;
stored_cleanup_reply(_Mailbox, error, _Archive, _Uploads) ->
    {error, offline_state_purge_failed};
stored_cleanup_reply(_Mailbox, _Offline, error, _Uploads) ->
    {error, archive_state_purge_failed};
stored_cleanup_reply(_Mailbox, _Offline, _Archive, {error, _} = Error) ->
    Error;
stored_cleanup_reply(_, _, _, _) -> {error, stored_state_purge_failed}.

purge_removed_mailbox(Removed, Host, MeshID) ->
    RemovedResources = lists:usort(
                         [{User, Host, MeshID}
                          || {User, MemberHost} <- Removed,
                             MemberHost =:= Host]),
    Purge = fun() -> purge_removed_mailbox_resources_tx(RemovedResources) end,
    case mnesia:transaction(Purge, [], 3) of
        {atomic, ok} -> ok;
        _ -> {error, mailbox_state_purge_failed}
    end.

purge_removed_mailbox_resources_tx(RemovedResources) ->
    mnesia:foldl(
      fun(#cynapsa_mesh_mailbox{} = Row, Acc) ->
              case mailbox_row_matches_removed_resource(Row,
                                                        RemovedResources) of
                  true -> delete_mailbox_row_tx(Row);
                  false -> ok
              end,
              Acc;
         (_, _Acc) -> mnesia:abort(invalid_mailbox_row)
      end, ok, cynapsa_mesh_mailbox).

mailbox_row_matches_removed_resource(
  #cynapsa_mesh_mailbox{owner = Owner, sender = Sender},
  RemovedResources) when is_list(RemovedResources) ->
    lists:member(Owner, RemovedResources)
        orelse lists:member(Sender, RemovedResources).

purge_removed_uploads(Removed, Host, MeshID) ->
    Owners = lists:usort([{User, Host, MeshID}
                          || {User, MemberHost} <- Removed,
                             MemberHost =:= Host]),
    Read = fun() ->
                   mnesia:foldl(
                     fun(#cynapsa_mesh_upload_object{owner = Owner} = Object,
                         Acc) ->
                             case lists:member(upload_owner_identity(Owner),
                                               Owners) of
                                 true -> [Object | Acc];
                                 false -> Acc
                             end;
                        (_, _Acc) -> mnesia:abort(invalid_upload_object)
                     end, [], cynapsa_mesh_upload_object)
           end,
    case mnesia:transaction(Read, [], 3) of
        {atomic, Objects} ->
            cleanup_reply([cleanup_removed_upload_record(
                             Object, Owners, Host)
                           || Object <- Objects]);
        _ -> {error, upload_ownership_storage_failure}
    end.

upload_owner_identity({User, Host, MeshID, pending, _Size, _Token,
                       _Expiry}) -> {User, Host, MeshID};
upload_owner_identity({User, Host, MeshID, completed, _Size}) ->
    {User, Host, MeshID};
upload_owner_identity({User, Host, MeshID, cleanup_pending, _Size}) ->
    {User, Host, MeshID};
upload_owner_identity({User, Host, MeshID}) -> {User, Host, MeshID};
upload_owner_identity(_) -> invalid.

cleanup_removed_upload_record(
  #cynapsa_mesh_upload_object{key = Key, path = Path} = Snapshot,
  Owners, Host) ->
    case catch persisted_upload_cleanup_safe(Snapshot, Host) of
        true ->
            case with_upload_path_lock(
                   Path,
                   fun() ->
                           cleanup_removed_upload_path_locked(
                             fun() -> read_current_removed_upload(
                                        Key, Path, Owners, Host)
                             end,
                             fun(#cynapsa_mesh_upload_object{slot = Slot}) ->
                                     cleanup_upload_object_locked(
                                       Host, Path, Slot)
                             end,
                             fun delete_current_removed_upload/1)
                   end) of
                {ok, Result} -> Result;
                {error, _} -> {error, upload_path_lock_failed}
            end;
        _ -> {error, invalid_upload_object_path}
    end.

cleanup_removed_upload_path_locked(Read, Cleanup, Delete)
  when is_function(Read, 0), is_function(Cleanup, 1),
       is_function(Delete, 1) ->
    case Read() of
        {ok, Current} ->
            case Cleanup(Current) of
                ok -> Delete(Current);
                Error -> Error
            end;
        absent -> ok;
        preserve -> ok;
        {error, _} = Error -> Error;
        _ -> {error, upload_ownership_storage_failure}
    end.

read_current_removed_upload(Key, Path, Owners, Host) ->
    Read = fun() ->
                   case mnesia:read(cynapsa_mesh_upload_object, Key, write) of
                       [] -> absent;
                       [#cynapsa_mesh_upload_object{
                           key = Key, owner = Owner, path = Path} = Current] ->
                           case upload_owner_identity(Owner) of
                               invalid -> mnesia:abort(invalid_upload_object);
                               Identity ->
                                   case lists:member(Identity, Owners) of
                                       true ->
                                           case catch
                                                  persisted_upload_cleanup_safe(
                                                    Current, Host) of
                                               true -> {ok, Current};
                                               _ -> mnesia:abort(
                                                      invalid_upload_object)
                                           end;
                                       false -> preserve
                                   end
                           end;
                       [_] -> mnesia:abort(invalid_upload_object)
                   end
           end,
    case mnesia:transaction(Read, [], 3) of
        {atomic, Result} -> Result;
        _ -> {error, upload_ownership_storage_failure}
    end.

delete_current_removed_upload(
  #cynapsa_mesh_upload_object{key = Key} = Current) ->
    Delete = fun() ->
                     case mnesia:read(cynapsa_mesh_upload_object, Key,
                                      write) of
                         [Current] -> mnesia:delete_object(Current), ok;
                         [] -> ok;
                         [_] -> mnesia:abort(upload_ownership_changed)
                     end
             end,
    case mnesia:transaction(Delete, [], 3) of
        {atomic, ok} -> ok;
        _ -> {error, upload_ownership_storage_failure}
    end.

cleanup_upload_object_locked(Host, Path, Slot) ->
    Proc = mod_http_upload:get_proc_name(Host, mod_http_upload),
    case whereis(Proc) of
        Pid when is_pid(Pid) -> cleanup_upload_object_locked(
                                  Host, Path, Slot, Pid);
        _ -> {error, upload_service_unavailable}
    end.

cleanup_upload_object_locked(_Host, Path, Slot, Pid) ->
    Pid ! {timeout, make_ref(), Slot},
    case file:delete(Path) of
        ok -> cleanup_upload_directory(Path);
        {error, enoent} -> ok;
        _ -> {error, upload_object_delete_failed}
    end.

safe_upload_object_path(Host, Path, [UserDir, RandDir, Filename] = Slot)
  when is_binary(Host), is_binary(Path) ->
    case valid_upload_path_component(UserDir)
         andalso valid_upload_path_component(RandDir)
         andalso valid_upload_filename_component(Filename) of
        true ->
            DocRoot0 = mod_http_upload_opt:docroot(Host),
            DocRoot1 = mod_http_upload:expand_home(DocRoot0),
            DocRoot = mod_http_upload:expand_host(DocRoot1, Host),
            Path =:= str:join([DocRoot | Slot], <<$/>>);
        false -> false
    end;
safe_upload_object_path(_Host, _Path, _Slot) -> false.

valid_upload_filename_component(<<"cynapsa-",
                                  Random:?UPLOAD_RANDOM_BYTES/binary,
                                  ".bin">>) -> upload_random(Random);
valid_upload_filename_component(_) -> false.

cleanup_upload_directory(Path) ->
    case file:del_dir(filename:dirname(Path)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, eexist} -> ok;
        {error, enotempty} -> ok;
        _ -> {error, upload_directory_cleanup_failed}
    end.

purge_table_if_present(Table, Predicate) ->
    case catch mnesia:table_info(Table, where_to_read) of
        {'EXIT', _} -> error;
        nowhere -> error;
        _ ->
            case mnesia:transaction(
                   fun() ->
                           mnesia:foldl(
                             fun(Record, Count) ->
                                     case Predicate(Record) of
                                         true -> mnesia:delete_object(Record),
                                                 Count + 1;
                                         false -> Count
                                     end
                             end, 0, Table)
                   end, [], 3) of
                {atomic, _} -> ok;
                {aborted, _} -> error
            end
    end.

removed_archive(Owner, {PeerUser, Host, MeshID}, Packet, Removed, Host,
                MeshID) ->
    lists:member({PeerUser, Host}, Removed)
        orelse (lists:member({Owner, Host}, Removed)
                andalso packet_mesh_scoped(Packet, Host, MeshID));
removed_archive(Owner, _Peer, Packet, Removed, Host, MeshID) ->
    lists:member({Owner, Host}, Removed)
        andalso packet_mesh_scoped(Packet, Host, MeshID).

packet_mesh_scoped(Packet, Host, MeshID) ->
    case packet_jids(Packet) of
        {ok, From, To} ->
            removed_mesh_packet(From, To, [], Host, MeshID) =:= scoped;
        error -> false
    end.

packet_removed_mesh(Packet, Removed, Host, MeshID) ->
    case packet_jids(Packet) of
        {ok, From, To} ->
            removed_mesh_packet(From, To, Removed, Host, MeshID) =:= true;
        error -> false
    end.

packet_jids(#xmlel{attrs = Attrs}) ->
    case {xml_attr(<<"from">>, Attrs), xml_attr(<<"to">>, Attrs)} of
        {From, To} when is_binary(From), is_binary(To),
                        From =/= <<>>, To =/= <<>> ->
            try {ok, jid:decode(From), jid:decode(To)}
            catch _:_ -> error
            end;
        _ -> error
    end;
packet_jids(Packet) ->
    try {ok, xmpp:get_from(Packet), xmpp:get_to(Packet)}
    catch _:_ -> error
    end.

removed_mesh_packet(From, To, [], Host, MeshID) ->
    case {mesh_endpoint(From, Host, MeshID), mesh_endpoint(To, Host, MeshID)} of
        {{ok, _}, {ok, _}} -> scoped;
        _ -> false
    end;
removed_mesh_packet(From, To, Removed, Host, MeshID) ->
    case {mesh_endpoint(From, Host, MeshID), mesh_endpoint(To, Host, MeshID)} of
        {{ok, FromUser}, {ok, ToUser}} ->
            lists:member({FromUser, Host}, Removed)
                orelse lists:member({ToUser, Host}, Removed);
        _ -> false
    end.

mesh_endpoint(#jid{luser = User, lserver = Host, lresource = MeshID}, Host,
              MeshID) when User =/= <<>> -> {ok, User};
mesh_endpoint(_, _Host, _MeshID) -> error.

await_sessions_closed(_Users, _Host, _Resource, 0) ->
    {error, session_revocation_timeout};
await_sessions_closed(Users, Host, Resource, Attempts) ->
    case lists:any(fun(User) ->
                           ejabberd_sm:get_session_pid(User, Host, Resource)
                               =/= none
                   end, Users) of
        false -> ok;
        true ->
            receive after 100 -> ok end,
            await_sessions_closed(Users, Host, Resource, Attempts - 1)
    end.

fence_existing_host_sessions(Host) ->
    Sessions = try ejabberd_sm:get_vh_session_list(Host)
               catch _:_ -> error
               end,
    SessionSIDs = fun(User, Server, Resource) ->
                          ejabberd_sm:get_session_sids(User, Server, Resource)
                  end,
    Close = fun(Pid, Reason) -> ejabberd_c2s:close(Pid, Reason) end,
    fence_existing_host_sessions(Host, Sessions, SessionSIDs, Close).

fence_existing_host_sessions(Host, Sessions, SessionSIDs, Close)
  when is_binary(Host), is_list(Sessions), is_function(SessionSIDs, 3),
       is_function(Close, 2) ->
    Exact = lists:usort([{User, Server, Resource}
                         || {User, Server, Resource} <- Sessions,
                            Server =:= Host,
                            is_binary(User), User =/= <<>>,
                            is_binary(Resource), Resource =/= <<>>]),
    try lists:foreach(
          fun({User, Server, Resource}) ->
                  SIDs = SessionSIDs(User, Server, Resource),
                  true = is_list(SIDs),
                  lists:foreach(
                    fun({_Stamp, Pid}) when is_pid(Pid) ->
                            _ = Close(Pid, authority_state_reset),
                            ok;
                       (_) -> erlang:error(invalid_session_owner)
                    end, SIDs)
          end, Exact) of
        ok -> ok
    catch _:_ -> {error, authority_session_fence_failed}
    end;
fence_existing_host_sessions(_Host, _Sessions, _SessionSIDs, _Close) ->
    {error, authority_session_fence_failed}.

proc(Host) ->
    gen_mod:get_module_proc(Host, ?MODULE).

%%%===================================================================
%%% Pure contract tests (the pinned release image intentionally omits EUnit)
%%%===================================================================

-ifdef(TEST).
test_removed_upload_current_row_fence() ->
    Parent = self(),
    Host = <<"mesh.test">>,
    Path = <<"/put-transition-before-removal">>,
    Key = {Host, Path},
    Slot = [<<"A">>, <<"A">>,
            <<"cynapsa-AAAAAAAAAAAAAAAAAAAAAAAAAA.bin">>],
    Pending = #cynapsa_mesh_upload_object{
                 key = Key,
                 owner = {<<"a">>, Host, <<"mesh-1">>, pending, 4,
                          <<1:128>>, 1000},
                 path = Path, slot = Slot},
    Completed = Pending#cynapsa_mesh_upload_object{
                  owner = {<<"a">>, Host, <<"mesh-1">>, completed, 4}},
    Table = ets:new(removed_upload_current_row, [set, public]),
    true = ets:insert(Table, {current, Pending}),
    Holder = spawn(
               fun() ->
                       Result = with_upload_path_lock(
                                  Path,
                                  fun() ->
                                          Parent ! removed_put_lock_won,
                                          receive complete_put -> ok end,
                                          true = ets:insert(
                                                   Table,
                                                   {current, Completed}),
                                          Parent ! removed_put_completed,
                                          ok
                                  end),
                       Parent ! {removed_put_holder_done, Result}
               end),
    receive removed_put_lock_won -> ok
    after 1000 -> erlang:error(removed_put_lock_timeout)
    end,
    _Waiter = spawn(
                fun() ->
                        Result = with_upload_path_lock(
                                   Path,
                                   fun() ->
                                           cleanup_removed_upload_path_locked(
                                             fun() ->
                                                     [{current, Current}] =
                                                         ets:lookup(
                                                           Table, current),
                                                     {ok, Current}
                                             end,
                                             fun(Current) ->
                                                     Parent !
                                                       {removed_cleaned,
                                                        Current},
                                                     ok
                                             end,
                                             fun(Current) ->
                                                     Parent !
                                                       {removed_deleted,
                                                        Current},
                                                     true = ets:delete(
                                                              Table,
                                                              current),
                                                     ok
                                             end)
                                   end),
                        Parent ! {removed_waiter_done, Result}
                end),
    receive {removed_cleaned, _} -> erlang:error(removed_lock_bypassed)
    after 50 -> ok
    end,
    Holder ! complete_put,
    receive removed_put_completed -> ok
    after 1000 -> erlang:error(removed_put_transition_timeout)
    end,
    receive {removed_put_holder_done, {ok, ok}} -> ok
    after 1000 -> erlang:error(removed_put_release_timeout)
    end,
    receive {removed_cleaned, Completed} -> ok
    after 1000 -> erlang:error(removed_current_cleanup_timeout)
    end,
    receive {removed_deleted, Completed} -> ok
    after 1000 -> erlang:error(removed_current_delete_timeout)
    end,
    receive {removed_waiter_done, {ok, ok}} -> ok
    after 1000 -> erlang:error(removed_waiter_timeout)
    end,
    [] = ets:lookup(Table, current),
    MustNotRun = fun(_Current) ->
                         Parent ! unexpected_removed_upload_action,
                         ok
                 end,
    ok = cleanup_removed_upload_path_locked(
           fun() -> preserve end, MustNotRun, MustNotRun),
    receive unexpected_removed_upload_action ->
                erlang:error(preserved_upload_owner_was_touched)
    after 0 -> ok
    end,
    {error, upload_object_delete_failed} =
        cleanup_removed_upload_path_locked(
          fun() -> {ok, Completed} end,
          fun(_Current) -> {error, upload_object_delete_failed} end,
          MustNotRun),
    receive unexpected_removed_upload_action ->
                erlang:error(failed_removed_cleanup_deleted_owner)
    after 0 -> ok
    end,
    true = ets:delete(Table),
    ok.

test_paused_upload_path_fence() ->
    Parent = self(),
    Path = <<"/paused-use-slot-removal">>,
    Holder = spawn(
               fun() ->
                       Result = with_upload_path_lock(
                                  Path,
                                  fun() ->
                                          Parent ! upload_path_locked,
                                          receive release_upload_path -> ok end
                                  end),
                       Parent ! {upload_path_holder_done, Result}
               end),
    receive upload_path_locked -> ok after 1000 -> erlang:error(lock_timeout) end,
    _Waiter = spawn(
                fun() ->
                        Parent ! upload_path_waiting,
                        Result = with_upload_path_lock(
                                   Path,
                                   fun() -> Parent ! upload_path_entered,
                                            removal_cleanup
                                   end),
                        Parent ! {upload_path_waiter_done, Result}
                end),
    receive upload_path_waiting -> ok after 1000 -> erlang:error(wait_timeout) end,
    receive upload_path_entered -> erlang:error(lock_bypassed)
    after 50 -> ok
    end,
    Holder ! release_upload_path,
    receive {upload_path_holder_done, {ok, ok}} -> ok
    after 1000 -> erlang:error(holder_release_timeout)
    end,
    receive upload_path_entered -> ok
    after 1000 -> erlang:error(cleanup_lock_timeout)
    end,
    receive {upload_path_waiter_done, {ok, removal_cleanup}} -> ok
    after 1000 -> erlang:error(cleanup_result_timeout)
    end.

test() ->
    {result, [?AUTHORITY_NAMESPACE, <<"urn:test:feature">>]} =
        disco_local_features(
          {result, [?AUTHORITY_NAMESPACE, <<"urn:test:feature">>,
                    ?AUTHORITY_NAMESPACE]}, undefined, undefined, <<>>, <<>>),
    {result, [<<"urn:test:feature">>]} =
        disco_local_features({result, [<<"urn:test:feature">>]}, undefined,
                             undefined, <<"other-node">>, <<>>),
    DiscoveryError = {error, authority_discovery_test},
    DiscoveryError = disco_local_features(DiscoveryError, undefined,
                                           undefined, <<>>, <<>>),
    {ok, <<"0123456789abcdef">>, 0} = parse_authority_sync(
      #xmlel{name = <<"sync">>,
             attrs = [{<<"xmlns">>, ?AUTHORITY_NAMESPACE},
                      {<<"nonce">>, <<"0123456789abcdef">>},
                      {<<"cursor">>, <<"0">>}], children = []}),
    A = jid:make(<<"a">>, <<"mesh.test">>, <<"mesh-1">>),
    B = jid:make(<<"b">>, <<"mesh.test">>, <<"mesh-1">>),
    Sync = authority_sync_element(<<"0123456789abcdef">>, 0, 2, [A, B]),
    SyncWire = fxml:element_to_binary(Sync),
    nomatch = binary:match(SyncWire, <<"generation">>),
    nomatch = binary:match(SyncWire, <<"watermark">>),
    true = removed_mesh_packet(A, B, [{<<"a">>, <<"mesh.test">>}],
                               <<"mesh.test">>, <<"mesh-1">>),
    false = removed_mesh_packet(A, B, [{<<"c">>, <<"mesh.test">>}],
                                <<"mesh.test">>, <<"mesh-1">>),
    true = pending_snapshot_capacity(0, 0, 0, 257, 8192),
    false = pending_snapshot_capacity(1, 257, 8192, 257, 8192),
    true = pending_snapshot_count_available(0),
    false = pending_snapshot_count_available(1),
    ReusedID = <<"cynapsa-upload-AAAAAAAAAAAAAAAAAAAAAAAAAA">>,
    {<<"mesh.test">>, <<"/one">>} =
        upload_object_key(<<"mesh.test">>, <<"/one">>, ReusedID),
    false = upload_object_key(<<"mesh.test">>, <<"/one">>, ReusedID)
        =:= upload_object_key(<<"mesh.test">>, <<"/two">>, ReusedID),
    true = retained_upload_capacity(0, 0, 0, 0, 1),
    false = retained_upload_capacity(?MAX_RETAINED_UPLOAD_OBJECTS,
                                     0, 0, 0, 1),
    false = retained_upload_capacity(0, ?MAX_RETAINED_UPLOAD_BYTES,
                                     0, 0, 1),
    false = retained_upload_capacity(0, 0, ?MAX_OWNER_UPLOAD_OBJECTS,
                                     0, 1),
    false = retained_upload_capacity(0, 0, 0,
                                     ?MAX_OWNER_UPLOAD_BYTES, 1),
    {ok, ?MAX_UPLOAD_BYTES} = parse_decimal(
                                <<"134217760">>, ?MAX_UPLOAD_BYTES),
    error = parse_decimal(<<"134217761">>, ?MAX_UPLOAD_BYTES),
    UploadToken = <<1:128>>,
    persistent_term:put(?UPLOAD_INSTANCE_KEY, UploadToken),
    PendingOwner = {<<"a">>, <<"mesh.test">>, <<"mesh-1">>, pending,
                    4, UploadToken,
                    erlang:monotonic_time(millisecond) + 10000},
    true = upload_owner_http_allowed(PendingOwner, <<"mesh.test">>, 'PUT'),
    false = upload_owner_http_allowed(PendingOwner, <<"mesh.test">>, 'GET'),
    CompletedOwner = {<<"a">>, <<"mesh.test">>, <<"mesh-1">>, completed,
                      4},
    true = upload_owner_http_allowed(CompletedOwner, <<"mesh.test">>, 'GET'),
    false = upload_owner_http_allowed(CompletedOwner, <<"mesh.test">>, 'PUT'),
    ExpiredObject = #cynapsa_mesh_upload_object{
                      key = {<<"mesh.test">>, <<"/expired">>},
                      owner = {<<"a">>, <<"mesh.test">>, <<"mesh-1">>,
                               pending, 4, UploadToken, 1},
                      path = <<"/expired">>, slot = []},
    true = upload_object_expired(ExpiredObject, <<"mesh.test">>,
                                 UploadToken, 1),
    CleanupPendingObject = ExpiredObject#cynapsa_mesh_upload_object{
                             owner = {<<"a">>, <<"mesh.test">>, <<"mesh-1">>,
                                      cleanup_pending, 4}},
    true = upload_object_expired(CleanupPendingObject, <<"mesh.test">>,
                                 UploadToken, 0),
    ValidFilename = <<"cynapsa-AAAAAAAAAAAAAAAAAAAAAAAAAA.bin">>,
    ValidSlot = [<<"safe">>, <<"safe">>, ValidFilename],
    MalformedPathObject =
        CleanupPendingObject#cynapsa_mesh_upload_object{
          key = {<<"mesh.test">>, malformed}, path = malformed,
          slot = ValidSlot},
    {error, invalid_upload_object_path} = cleanup_expired_upload_object(
                                            MalformedPathObject,
                                            <<"mesh.test">>, UploadToken, 0),
    MalformedSlotObject =
        CleanupPendingObject#cynapsa_mesh_upload_object{
          key = {<<"mesh.test">>, <<"/outside-upload-root">>},
          path = <<"/outside-upload-root">>, slot = [<<"..">>]},
    {error, invalid_upload_object_path} = cleanup_expired_upload_object(
                                            MalformedSlotObject,
                                            <<"mesh.test">>, UploadToken, 0),
    UploadPath = <<"/owned">>,
    UploadKey = {<<"mesh.test">>, UploadPath},
    OwnedObject = #cynapsa_mesh_upload_object{
                    key = UploadKey, owner = CompletedOwner,
                    path = UploadPath, slot = []},
    MustNotCleanup = fun() -> self() ! unexpected_upload_cleanup, ok end,
    ok = cancel_unowned_upload_path_locked(
           [OwnedObject], UploadKey, UploadPath, MustNotCleanup),
    ok = cancel_unowned_upload_path_locked(
           [OwnedObject], UploadKey, UploadPath, MustNotCleanup),
    receive unexpected_upload_cleanup ->
                erlang:error(owned_upload_path_was_cancelled)
    after 0 -> ok
    end,
    DoCleanup = fun() -> self() ! expected_upload_cleanup, ok end,
    ok = cancel_unowned_upload_path_locked(
           [], UploadKey, UploadPath, DoCleanup),
    receive expected_upload_cleanup -> ok
    after 0 -> erlang:error(unowned_upload_path_was_not_cancelled)
    end,
    CleanupFailure = fun() -> {error, upload_object_delete_failed} end,
    MustNotDelete = fun(_Object) ->
                            self() ! unexpected_upload_record_delete,
                            ok
                    end,
    {error, upload_object_delete_failed} = finish_failed_upload_cleanup(
                                              OwnedObject, CleanupFailure,
                                              MustNotDelete),
    receive unexpected_upload_record_delete ->
                erlang:error(failed_cleanup_deleted_upload_owner)
    after 0 -> ok
    end,
    persistent_term:erase(?UPLOAD_INSTANCE_KEY),
    ok = test_removed_upload_current_row_fence(),
    ok = test_paused_upload_path_fence(),
    Pid = self(), SID = make_ref(), Nonce = <<"fedcba9876543210">>,
    Members = lists:duplicate(257, A),
    Bytes = snapshot_members_bytes(Members), Expiry = 5000,
    Pending = #{Pid => {SID, <<"a">>, <<"mesh-1">>, Nonce, Members,
                         256, Bytes, Expiry}},
    {ok, Members, [_], Continued} = authority_snapshot_page(
                                      <<"mesh.test">>, <<"mesh-1">>, Pid,
                                      SID, <<"a">>, Nonce, 256, Pending,
                                      257, Bytes, 4999),
    false = maps:is_key(Pid, Continued),
    {error, snapshot_expired, Pid, <<"a">>, <<"mesh-1">>} =
        authority_snapshot_page(<<"mesh.test">>, <<"mesh-1">>, Pid, SID,
                                <<"a">>, Nonce, 256, Pending, 257, Bytes,
                                Expiry),
    Ready = #{Pid => {SID, <<"a">>, <<"mesh-1">>}},
    true = exact_session_epoch_current(Pid, SID, <<"a">>, <<"mesh.test">>,
                                       <<"mesh-1">>, Ready,
                                       fun(_, _, _) -> Pid end),
    Replacement = spawn(fun() -> receive stop -> ok end end),
    false = exact_session_epoch_current(Pid, SID, <<"a">>, <<"mesh.test">>,
                                        <<"mesh-1">>, Ready,
                                        fun(_, _, _) -> Replacement end),
    Server = jid:make(<<>>, <<"mesh.test">>, <<>>),
    Policy0 = #message{type = error, from = Server, to = A,
                       sub_els = [#stanza_error{type = cancel,
                                                reason = 'policy-violation'}]},
    Policy = xmpp:put_meta(Policy0, ?ERROR_META,
                           {policy_error, Pid, SID, A}),
    true = safe_policy_error_for_session(
             Policy, {Pid, SID, A}, <<"mesh.test">>,
             fun(_, _, _) -> Pid end),
    false = safe_policy_error_for_session(
              Policy, {Pid, SID, A}, <<"mesh.test">>,
              fun(_, _, _) -> Replacement end),
    Replacement ! stop,
    Frame = #xmlel{name = <<"frame">>,
                   attrs = [{<<"xmlns">>, ?AZTM_NAMESPACE},
                            {<<"v">>, <<"1">>}],
                   children = [{xmlcdata, <<"owACAQECQWU">>}]},
    Eligible = #message{id = <<"msg_AAAAAAAAAAAAAAAAAAAAAA">>,
                        type = chat, from = A, to = B,
                        sub_els = [Frame]},
    eligible = cynapsa_message_kind(Eligible),
    transient = cynapsa_message_kind(Eligible#message{id = <<>>}),
    TransientFrame = Frame#xmlel{children = [{xmlcdata, <<"ogACAQQ">>}]},
    transient = cynapsa_message_kind(
                  Eligible#message{id = <<>>, sub_els = [TransientFrame]}),
    transient = cynapsa_message_kind(
                  Eligible#message{id = <<"367a1cb35a378489">>,
                                   sub_els = [TransientFrame]}),
    eligible = cynapsa_message_kind(
                  Eligible#message{sub_els = [TransientFrame]}),
    malformed = cynapsa_message_kind(
                  Eligible#message{sub_els = [Frame#xmlel{
                                                attrs = [{<<"xmlns">>,
                                                          ?AZTM_NAMESPACE},
                                                         {<<"v">>, <<"2">>}]}]}),
    ordinary = cynapsa_message_kind(
                 Eligible#message{sub_els = [#xmlel{
                                                name = <<"probe">>,
                                                attrs = [{<<"xmlns">>,
                                                          <<"urn:test">>}]}]}),
    {true, <<"==">>} = canonical_raw_base64_shape(<<"TQ">>),
    false = canonical_raw_base64_shape(<<"TR">>),
    {true, <<"=">>} = canonical_raw_base64_shape(<<"TWE">>),
    false = canonical_raw_base64_shape(<<"TWF">>),
    true = sm_handled_not_ahead(0, 0),
    true = sm_handled_not_ahead(4294967295, 0),
    false = sm_handled_not_ahead(1, 0),
    false = sm_handled_not_ahead(0, 4294967295),
    MailboxKey = {{<<"b">>, <<"mesh.test">>, <<"mesh-1">>}, 7},
    Leased = xmpp:put_meta(Eligible, ?MAILBOX_META, MailboxKey),
    AckQueue0 = p1_queue:new(),
    AckQueue1 = p1_queue:in({3, 0, Leased}, AckQueue0),
    AckQueue2 = p1_queue:in({4, 0, Eligible}, AckQueue1),
    [MailboxKey] = acknowledged_mailbox_keys(AckQueue2, 3),
    [] = acknowledged_mailbox_keys(AckQueue2, 2),
    WrapKey = {{<<"b">>, <<"mesh.test">>, <<"mesh-1">>}, 8},
    WrapLeased = xmpp:put_meta(Eligible, ?MAILBOX_META, WrapKey),
    WrapQueue0 = p1_queue:new(),
    WrapQueue1 = p1_queue:in({4294967295, 0, Leased}, WrapQueue0),
    WrapQueue2 = p1_queue:in({0, 0, WrapLeased}, WrapQueue1),
    [MailboxKey, WrapKey] = acknowledged_mailbox_keys(WrapQueue2, 0),
    [] = acknowledged_mailbox_keys(WrapQueue2, 1),
    OldResumePid = spawn(fun() -> receive stop -> ok end end),
    OldResumeSID = {make_ref(), OldResumePid},
    NewResumePid = self(),
    ResumeReady = #{OldResumePid =>
                        {OldResumeSID, <<"b">>, <<"mesh-1">>}},
    {ok, ResumeToken} = prepare_resume_transfer(
                          OldResumePid, OldResumeSID, NewResumePid,
                          <<"b">>, <<"mesh.test">>, <<"mesh-1">>,
                          ResumeReady,
                          fun(_, _, _) -> OldResumePid end),
    ResumeTransfers = #{NewResumePid =>
                            {ResumeToken, OldResumePid, OldResumeSID,
                             <<"b">>, <<"mesh-1">>, ready}},
    NewResumeSID = {make_ref(), NewResumePid},
    {ok, ready} = complete_resume_transfer(
                    NewResumePid, NewResumeSID, <<"b">>, <<"mesh.test">>,
                    <<"mesh-1">>, ResumeToken, ResumeTransfers,
                    fun(_, _, _) -> NewResumePid end),
    NotReadyToken = make_ref(),
    NotReadyTransfers = #{NewResumePid =>
                              {NotReadyToken, OldResumePid, OldResumeSID,
                               <<"b">>, <<"mesh-1">>, not_ready}},
    {ok, not_ready} = complete_resume_transfer(
                        NewResumePid, NewResumeSID, <<"b">>,
                        <<"mesh.test">>, <<"mesh-1">>, NotReadyToken,
                        NotReadyTransfers,
                        fun(_, _, _) -> NewResumePid end),
    error = complete_resume_transfer(
              NewResumePid, NewResumeSID, <<"b">>, <<"mesh.test">>,
              <<"mesh-1">>, make_ref(), ResumeTransfers,
              fun(_, _, _) -> NewResumePid end),
    not_ready = prepare_resume_transfer(
                  OldResumePid, OldResumeSID, NewResumePid,
                  <<"b">>, <<"mesh.test">>, <<"mesh-1">>, ResumeReady,
                  fun(_, _, _) -> none end),
    {ok, <<"mesh.test">>, OldResumePid, OldResumeSID, <<"b">>,
     <<"mesh-1">>} = resume_handoff_identity(
                       #{jid => B}, #{jid => B, sid => OldResumeSID},
                       NewResumePid),
    error = resume_handoff_identity(
              #{jid => B}, #{jid => A, sid => OldResumeSID}, NewResumePid),
    OtherToken = make_ref(),
    ResumeState = #state{
                    ready = ResumeReady,
                    resume_transfers = #{
                      NewResumePid =>
                          {ResumeToken, OldResumePid, OldResumeSID,
                           <<"b">>, <<"mesh-1">>, ready},
                      OldResumePid =>
                          {OtherToken, NewResumePid, NewResumeSID,
                           <<"b">>, <<"mesh-2">>, not_ready}}},
    InvalidatedResumeState = invalidate_mesh(<<"mesh-1">>, ResumeState),
    false = maps:is_key(OldResumePid,
                        InvalidatedResumeState#state.ready),
    false = maps:is_key(NewResumePid,
                        InvalidatedResumeState#state.resume_transfers),
    true = maps:is_key(OldResumePid,
                       InvalidatedResumeState#state.resume_transfers),
    #iq{type = result, id = ReadyResultID, from = Server, to = B,
        sub_els = [#xmlel{name = <<"resume-authority">>,
                          attrs = ReadyResultAttrs,
                          children = []}]} =
        resume_authority_result(<<"mesh.test">>, <<"b">>, <<"mesh-1">>,
                                ready),
    {0, _} = binary:match(ReadyResultID,
                          <<"cynapsa-resume-authority-">>),
    {ok, ReadyResultValues} = exact_attrs(
                                ReadyResultAttrs,
                                [<<"xmlns">>, <<"status">>]),
    ?AUTHORITY_NAMESPACE = maps:get(<<"xmlns">>, ReadyResultValues),
    <<"ready">> = maps:get(<<"status">>, ReadyResultValues),
    #iq{type = result, id = NotReadyResultID, from = Server, to = B,
        sub_els = [#xmlel{name = <<"resume-authority">>,
                          attrs = NotReadyResultAttrs,
                          children = []}]} =
        resume_authority_result(<<"mesh.test">>, <<"b">>, <<"mesh-1">>,
                                not_ready),
    false = ReadyResultID =:= NotReadyResultID,
    {ok, NotReadyResultValues} = exact_attrs(
                                   NotReadyResultAttrs,
                                   [<<"xmlns">>, <<"status">>]),
    <<"not-ready">> = maps:get(<<"status">>, NotReadyResultValues),
    %% Membership controls are owned by the exact IQ ID, c2s PID, SID, and
    %% canonical bound full JID. No subset can consume another session's
    %% pending control.
    ControlPid = spawn(fun() -> receive stop -> ok end end),
    ControlSID = make_ref(),
    ControlID = <<"cynapsa-membership-test">>,
    ControlExpiry = 100,
    ControlPending = #{ControlID =>
                           {ControlPid, ControlSID, A, ControlExpiry}},
    {ok, #{}} = consume_control_ack(ControlPid, ControlSID, A, ControlID,
                                    ControlPending),
    error = consume_control_ack(ControlPid, make_ref(), A, ControlID,
                                ControlPending),
    error = consume_control_ack(ControlPid, ControlSID, B, ControlID,
                                ControlPending),
    error = consume_control_ack(ControlPid, ControlSID, A,
                                <<"cynapsa-membership-other">>,
                                ControlPending),
    ReservedControls = transfer_control_owner(
                         ControlPending, ControlPid, ControlSID,
                         NewResumePid, ControlSID, A),
    {NewResumePid, ControlSID, A, ControlExpiry} =
        maps:get(ControlID, ReservedControls),
    CompletedControls = transfer_control_owner(
                          ReservedControls, NewResumePid, ControlSID,
                          NewResumePid, NewResumeSID, A),
    {NewResumePid, NewResumeSID, A, ControlExpiry} =
        maps:get(ControlID, CompletedControls),

    Close = fun(ClosePid, Reason) ->
                    self() ! {control_closed, ClosePid, Reason},
                    ok
            end,
    #state{pending_controls = #{}} = expire_control_acks(
                                        ControlExpiry,
                                        #state{pending_controls =
                                                   ControlPending}, Close),
    receive
        {control_closed, ControlPid, authority_control_ack_timeout} -> ok
    after 0 -> erlang:error(control_ack_timeout_did_not_close_owner)
    end,

    QueuePid = spawn(fun() -> receive stop -> ok end end),
    QueueSID = make_ref(),
    CurrentQueuePid = fun(_, _, _) -> QueuePid end,
    Route = fun(#iq{id = RoutedID, to = RoutedTo}) ->
                    self() ! {control_routed, RoutedID, RoutedTo},
                    ok
            end,
    QueuedControls = queue_membership_controls(
                       [{QueuePid, QueueSID, <<"a">>, <<"mesh-1">>}],
                       <<"mesh.test">>, #{}, 0, 5000, 1, Route, Close,
                       CurrentQueuePid),
    [{QueuedID, {QueuePid, QueueSID, A, 5000}}] =
        maps:to_list(QueuedControls),
    receive
        {control_routed, QueuedID, A} -> ok
    after 0 -> erlang:error(control_was_not_routed)
    end,
    CapacityPid = spawn(fun() -> receive stop -> ok end end),
    QueuedControls = queue_membership_controls(
                       [{CapacityPid, make_ref(), <<"b">>, <<"mesh-1">>}],
                       <<"mesh.test">>, QueuedControls, 0, 5000, 1,
                       Route, Close, fun(_, _, _) -> CapacityPid end),
    receive
        {control_closed, CapacityPid, authority_control_capacity} -> ok
    after 0 -> erlang:error(control_capacity_did_not_close_owner)
    end,
    FailurePid = spawn(fun() -> receive stop -> ok end end),
    #{} = queue_membership_controls(
            [{FailurePid, make_ref(), <<"a">>, <<"mesh-1">>}],
            <<"mesh.test">>, #{}, 0, 5000, 1,
            fun(_) -> {error, unavailable} end, Close,
            fun(_, _, _) -> FailurePid end),
    receive
        {control_closed, FailurePid, authority_control_enqueue_failed} -> ok
    after 0 -> erlang:error(control_route_failure_did_not_close_owner)
    end,
    ReplacedPid = spawn(fun() -> receive stop -> ok end end),
    #{} = queue_membership_controls(
            [{ReplacedPid, make_ref(), <<"a">>, <<"mesh-1">>}],
            <<"mesh.test">>, #{}, 0, 5000, 1, Route, Close,
            fun(_, _, _) -> none end),
    receive
        {control_closed, ReplacedPid, authority_control_session_replaced} -> ok
    after 0 -> erlang:error(replaced_control_owner_was_not_closed)
    end,
    ControlPid ! stop,
    QueuePid ! stop,
    CapacityPid ! stop,
    FailurePid ! stop,
    ReplacedPid ! stop,
    OldResumePid ! stop,
    %% Mailbox removal is scoped to the exact user/host/resource tuple. A
    %% second mesh resource for the same bare account is independent, while a
    %% row sent by or addressed to the removed resource is retired.
    RemovedResource = {<<"a">>, <<"mesh.test">>, <<"mesh-1">>},
    OtherResource = {<<"a">>, <<"mesh.test">>, <<"mesh-2">>},
    RetainedResource = {<<"b">>, <<"mesh.test">>, <<"mesh-1">>},
    true = mailbox_row_matches_removed_resource(
             #cynapsa_mesh_mailbox{owner = RemovedResource,
                                   sender = RetainedResource},
             [RemovedResource]),
    true = mailbox_row_matches_removed_resource(
             #cynapsa_mesh_mailbox{owner = RetainedResource,
                                   sender = RemovedResource},
             [RemovedResource]),
    false = mailbox_row_matches_removed_resource(
              #cynapsa_mesh_mailbox{owner = OtherResource,
                                    sender = RetainedResource},
              [RemovedResource]),
    RetentionValidator = mod_opt_type(mailbox_retention_seconds),
    ?MAX_MAILBOX_RETENTION_SECONDS =
        RetentionValidator(?MAX_MAILBOX_RETENTION_SECONDS),
    {'EXIT', {function_clause, _}} =
        (catch RetentionValidator(?MAX_MAILBOX_RETENTION_SECONDS + 1)),
    ok.

-endif.
