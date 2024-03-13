-module(ubidots_emqx_retainer_ecpool).

-author("ubidots").

-export([start_pools/3]).

init_redis_cluster(Pool,
                   Env,
                   EnvVariables,
                   ServerKey,
                   PasswordKey,
                   PoolSizeKey,
                   ReconnectKey) ->
    eredis_cluster:start(),
    Fun = fun(S) ->
             case string:split(S, ":", trailing) of
                 [Domain] ->
                     {Domain, 6379};
                 [Domain, Port] ->
                     {Domain, list_to_integer(Port)}
             end
          end,
    Server = maps:get(ServerKey, EnvVariables, 10),
    Servers = string:tokens(Server, ","),
    Password = maps:get(PasswordKey, EnvVariables, ""),
    eredis_cluster:start_pool(Pool,
                              [{pool_size, maps:get(PoolSizeKey, EnvVariables, 10)},
                               {password, Password},
                               {servers, [Fun(S1) || S1 <- Servers]},
                               {auto_reconnect, maps:get(ReconnectKey, Env, 3)}]).

start_pools(PoolReactor, PoolCore, Env) ->
    EnvVariables = ubidots_emqx_retainer_settings:get_settings(),
    start_reactor_pool(PoolReactor, Env, EnvVariables),
    start_core_pool(PoolCore, Env, EnvVariables).

start_reactor_pool(PoolReactor, Env, EnvVariables) ->
    Type = maps:get(reactor_cache_type, EnvVariables, single),
    case Type of
        cluster ->
            init_redis_cluster(PoolReactor,
                               Env,
                               EnvVariables,
                               reactor_cache_server,
                               reactor_cache_password,
                               reactor_cache_pool_size,
                               reactor_cache_reconnect);
        single ->
            ecpool:start_pool(PoolReactor,
                              ubidots_emqx_reactor_redis_cli,
                              get_ecpool_reactor_options(EnvVariables) ++ Env)
    end.

start_core_pool(PoolCore, Env, EnvVariables) ->
    Type = maps:get(ubidots_cache_type, EnvVariables, single),
    case Type of
        cluster ->
            init_redis_cluster(PoolCore,
                               Env,
                               EnvVariables,
                               ubidots_cache_server,
                               ubidots_cache_password,
                               ubidots_cache_pool_size,
                               ubidots_cache_reconnect);
        single ->
            ecpool:start_pool(PoolCore,
                              ubidots_emqx_core_redis_cli,
                              get_ecpool_ubidots_options(EnvVariables) ++ Env)
    end.

get_ecpool_reactor_options(EnvVariables) ->
    [{pool_size, maps:get(reactor_cache_pool_size, EnvVariables, 10)},
     {pool_type, maps:get(reactor_cache_pool_type, EnvVariables, round_robin)},
     {auto_reconnect, maps:get(reactor_cache_reconnect, EnvVariables, 3)}].

get_ecpool_ubidots_options(EnvVariables) ->
    [{pool_size, maps:get(ubidots_cache_pool_size, EnvVariables, 10)},
     {pool_type, maps:get(ubidots_cache_pool_type, EnvVariables, round_robin)},
     {auto_reconnect, maps:get(ubidots_cache_reconnect, EnvVariables, 3)}].
