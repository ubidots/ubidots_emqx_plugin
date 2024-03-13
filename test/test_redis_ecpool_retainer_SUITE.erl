-module(test_redis_ecpool_retainer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(POOL_REACTOR, pool_reactor).
-define(POOL_CORE, pool_core).

all() ->
    [{group, all}].

groups() ->
    [{all,
      [sequence],
      [t_retain_single_value,
       t_retain_lv,
       t_retain_empty,
       t_retain_wild_card_variable,
       t_retain_wild_card_variable_lv,
       t_retain_wild_card_variable_and_lv,
       t_retain_wild_card_devices]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ecpool),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(ecpool),
    ok = application:stop(gproc).

t_retain_wild_card_devices(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/#",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages =
        [emqx_message:make("/v1.6/devices/d1/v1/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v1",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v2/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v2",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v3/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v3",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d2/v1/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d2/v1",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d2/v2/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d2/v2",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d2/v3/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d2/v3",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>)],
    ?assertEqual(12, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_wild_card_variable_and_lv(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/#",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages =
        [emqx_message:make("/v1.6/devices/d1/v1/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v1",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v2/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v2",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v3/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v3",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>)],
    ?assertEqual(6, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_wild_card_variable_lv(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/+/lv",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages =
        [emqx_message:make("/v1.6/devices/d1/v1/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v2/lv", <<"12.1">>),
         emqx_message:make("/v1.6/devices/d1/v3/lv", <<"12.1">>)],
    ?assertEqual(3, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_wild_card_variable(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/+",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages =
        [emqx_message:make("/v1.6/devices/d1/v1",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v2",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
         emqx_message:make("/v1.6/devices/d1/v3",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>)],
    ?assertEqual(3, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_empty(_) ->
    Devices = [],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/v1",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages = [],
    ?assertEqual(0, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_lv(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/v1/lv",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages = [emqx_message:make("/v1.6/devices/d1/v1/lv", <<"12.1">>)],
    ?assertEqual(1, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

t_retain_single_value(_) ->
    Devices =
        ["d1",
         "d1_id",
         ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
         "d2",
         "d2_id",
         ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
    ReactorRedisClient = ubidots_emqx_retainer_test_utils:get_reactor_redis_client(#{}),
    UbidotsRedisClient = ubidots_emqx_retainer_test_utils:get_ubidots_redis_client(#{}),
    ubidots_emqx_retainer_test_utils:initialize_mqtt_cache(ReactorRedisClient,
                                                           single,
                                                           UbidotsRedisClient,
                                                           single,
                                                           "token",
                                                           "owner_id",
                                                           Devices),
    Env = [],
    ubidots_emqx_retainer_ecpool:start_pools(?POOL_REACTOR, ?POOL_CORE, Env),
    Topic = "/v1.6/users/token/devices/d1/v1",
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    Messages =
        ubidots_emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic,
                                                                               EnvVariables,
                                                                               ?POOL_REACTOR,
                                                                               ?POOL_CORE),
    ExpectedMessages =
        [emqx_message:make("/v1.6/devices/d1/v1",
                           <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>)],
    ?assertEqual(1, length(Messages)),
    validate_messages(ExpectedMessages, Messages),
    ubidots_emqx_retainer_test_utils:flushdb(ReactorRedisClient,
                                             single,
                                             UbidotsRedisClient,
                                             single).

validate_messages([], []) ->
    ok;
validate_messages([#message{topic = Topic, payload = Payload} = _Message | Rest],
                  [#message{topic = ExpectedTopic, payload = ExpectedPayload} = _ExpectedMessage
                   | ExpectedRest]) ->
    ?assertEqual(Topic, ExpectedTopic),
    ?assertEqual(Payload, ExpectedPayload),
    validate_messages(Rest, ExpectedRest).
