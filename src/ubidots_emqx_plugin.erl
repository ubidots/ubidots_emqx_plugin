-module(ubidots_emqx_plugin).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
%% for logging
-include_lib("emqx/include/logger.hrl").

-define(POOL_REACTOR, pool_reactor).
-define(POOL_CORE, pool_core).

-export([load/1, unload/0]).
%% Client Lifecycle Hooks
-export([on_client_subscribe/4, on_client_unsubscribe/4]).
%% Session Lifecycle Hooks
-export([on_session_subscribed/4]).
%% Message Pubsub Hooks
-export([on_message_delivered/3]).

%% Called when the plugin application start
load(Env) ->
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    hook('client.subscribe', {?MODULE, on_client_subscribe, [Env]}),
    hook('client.unsubscribe', {?MODULE, on_client_unsubscribe, [Env]}),
    hook('session.subscribed', {?MODULE, on_session_subscribed, [Env]}),
    hook('message.delivered', {?MODULE, on_message_delivered, [Env]}).

%%--------------------------------------------------------------------
%% Client Lifecycle Hooks
%%--------------------------------------------------------------------

on_client_subscribe(#{username := UserName}, _Properties, TopicFilters, _Env) ->
    NewTopicFilters =
        lists:map(fun(TopicFilter) ->
                     ubidots_emqx_topic_changer:add_users_topic(UserName, TopicFilter)
                  end,
                  TopicFilters),
    {ok, NewTopicFilters}.

on_client_unsubscribe(#{username := UserName}, _Properties, TopicFilters, _Env) ->
    NewTopicFilters =
        lists:map(fun(TopicFilter) ->
                     ubidots_emqx_topic_changer:add_users_topic(UserName, TopicFilter)
                  end,
                  TopicFilters),
    {ok, NewTopicFilters}.

%%--------------------------------------------------------------------
%% Session Lifecycle Hooks
%%--------------------------------------------------------------------

on_session_subscribed(_, _, #{share := ShareName}, _Env) when ShareName =/= undefined ->
    ok;
on_session_subscribed(_, Topic, _Message, Env) ->
    Config = #{pool_reactor => ?POOL_REACTOR, pool_core => ?POOL_CORE},
    emqx_pool:async_submit(fun dispatch/4, [self(), Topic, Env, Config]).

dispatch(Pid, Topic, _Env, #{pool_reactor := PoolReactor, pool_core := PoolCore}) ->
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    NewMessages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               PoolReactor,
                                                                               PoolCore),
    dispatch_ubidots_message(NewMessages, Pid).

dispatch_ubidots_message([], _) ->
    ok;
dispatch_ubidots_message([Msg = #message{topic = Topic} | Rest], Pid) ->
    Pid ! {deliver, Topic, Msg},
    dispatch_ubidots_message(Rest, Pid).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return

on_message_delivered(_ClientInfo, #message{topic = Topic} = Message, _Env) ->
    NewMessage = ubidots_emqx_topic_changer:remove_users_topic(Topic, Message),
    {ok, NewMessage}.

%% Called when the plugin application stop
unload() ->
    unhook('client.subscribe', {?MODULE, on_client_subscribe}),
    unhook('client.unsubscribe', {?MODULE, on_client_unsubscribe}),
    unhook('session.subscribed', {?MODULE, on_session_subscribed}),
    unhook('message.delivered', {?MODULE, on_message_delivered}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).
