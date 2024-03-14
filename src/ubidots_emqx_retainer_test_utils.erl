%%%-------------------------------------------------------------------
%%% @author ubidots
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(ubidots_emqx_retainer_test_utils).

-author("ubidots").

-include_lib("eunit/include/eunit.hrl").

-export([get_reactor_redis_client/1, get_ubidots_redis_client/1, initialize_mqtt_cache/7,
         init_redis_cluster_ubidots/2, init_redis_cluster_reactor/2, flushdb/4]).

execute_redis_command(_Pool, cluster, ["FLUSHDB"]) ->
    ok;
execute_redis_command(Pool, Type, Args) ->
    case Type of
        cluster ->
            {ok, Result} = eredis_cluster:q(Pool, Args),
            Result;
        single ->
            {ok, Result} = eredis:q(Pool, Args),
            Result
    end.

get_reactor_redis_client(Env) ->
    Host = maps:get(reactor_cache_host_name, Env, "127.0.0.1"),
    Port = maps:get(reactor_cache_port, Env, 6379),
    Database = maps:get(reactor_cache_database, Env, 2),
    Password = maps:get(reactor_cache_password, Env, ""),
    {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
    RedisClient.

get_ubidots_redis_client(Env) ->
    Type = maps:get(ubidots_cache_type, Env, single),
    Host = maps:get(ubidots_cache_host_name, Env, "127.0.0.1"),
    Port = maps:get(ubidots_cache_port, Env, 6379),
    Database = maps:get(ubidots_cache_database, Env, 1),
    Password = maps:get(ubidots_cache_password, Env, ""),
    case Type of
        single ->
            {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
            RedisClient;
        cluster ->
            ok
    end.

init_redis_cluster_reactor(PoolReactor, Env) ->
    init_redis_cluster(PoolReactor,
                       Env,
                       reactor_cache_server,
                       reactor_cache_pool_size,
                       reactor_cache_reconnect).

init_redis_cluster_ubidots(PoolCore, Env) ->
    init_redis_cluster(PoolCore,
                       Env,
                       ubidots_cache_server,
                       ubidots_cache_pool_size,
                       ubidots_cache_reconnect).

init_redis_cluster(Pool, Env, ServerKey, PoolSizeKey, ReconnectKey) ->
    eredis_cluster:start(),
    Fun = fun(S) ->
             case string:split(S, ":", trailing) of
                 [Domain] ->
                     {Domain, 6379};
                 [Domain, Port] ->
                     {Domain, list_to_integer(Port)}
             end
          end,
    Server = maps:get(ServerKey, Env, 10),
    Servers = string:tokens(Server, ","),
    eredis_cluster:start_pool(Pool,
                              [{pool_size, maps:get(PoolSizeKey, Env, 10)},
                               {servers, [Fun(S1) || S1 <- Servers]},
                               {auto_reconnect, maps:get(ReconnectKey, Env, 3)}]).

flushdb(ReactorRedisClient, ReactorRedisType, UbidotsRedisClient, UbidotsRedisType) ->
    execute_redis_command(ReactorRedisClient, ReactorRedisType, ["FLUSHDB"]),
    execute_redis_command(UbidotsRedisClient, UbidotsRedisType, ["FLUSHDB"]).

initialize_variables(_, _, _, _, _, _, []) ->
    ok;
initialize_variables(ReactorRedisClient,
                     ReactorRedisType,
                     UbidotsRedisClient,
                     UbidotsRedisType,
                     OwnerId,
                     DeviceLabel,
                     [VariableLabel, VariableId | Rest]) ->
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET",
                           "reactor_variable_data/" ++ VariableId,
                           "/variable_label",
                           VariableLabel]),
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET",
                           "reactor_variables/" ++ OwnerId,
                           "/" ++ DeviceLabel ++ "/" ++ VariableLabel,
                           VariableId]),
    ValueData =
        "{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": "
        "13}",
    execute_redis_command(UbidotsRedisClient,
                          UbidotsRedisType,
                          ["SET", "last_value_variables_json:" ++ VariableId, ValueData]),
    execute_redis_command(UbidotsRedisClient,
                          UbidotsRedisType,
                          ["SET", "last_value_variables_string:" ++ VariableId, "12.1"]),
    initialize_variables(ReactorRedisClient,
                         ReactorRedisType,
                         UbidotsRedisClient,
                         UbidotsRedisType,
                         OwnerId,
                         DeviceLabel,
                         Rest).

initialize_devices(_, _, _, _, _, _, []) ->
    ok;
initialize_devices(ReactorRedisClient,
                   ReactorRedisType,
                   UbidotsRedisClient,
                   UbidotsRedisType,
                   OwnerId,
                   Token,
                   [DeviceLabel, DeviceId, Variables | Rest]) ->
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET",
                           "reactor_device_data/" ++ DeviceId,
                           "/device_label",
                           DeviceLabel]),
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel, DeviceId]),
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["SADD",
                           "reactor_devices_with_permissions/view_value/" ++ Token,
                           DeviceId]),
    initialize_variables(ReactorRedisClient,
                         ReactorRedisType,
                         UbidotsRedisClient,
                         UbidotsRedisType,
                         OwnerId,
                         DeviceLabel,
                         Variables),
    initialize_devices(ReactorRedisClient,
                       ReactorRedisType,
                       UbidotsRedisClient,
                       UbidotsRedisType,
                       OwnerId,
                       Token,
                       Rest),
    ok.

initialize_mqtt_cache(ReactorRedisClient,
                      ReactorRedisType,
                      UbidotsRedisClient,
                      UbidotsRedisType,
                      Token,
                      OwnerId,
                      Devices) ->
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET", "reactor_tokens/" ++ Token, "/owner_id", OwnerId]),
    execute_redis_command(ReactorRedisClient,
                          ReactorRedisType,
                          ["HSET", "reactor_tokens/" ++ Token, "/permissions_type", "device"]),
    initialize_devices(ReactorRedisClient,
                       ReactorRedisType,
                       UbidotsRedisClient,
                       UbidotsRedisType,
                       OwnerId,
                       Token,
                       Devices),
    ok.
