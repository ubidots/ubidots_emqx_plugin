-module(ubidots_emqx_retainer_settings).

-author("ubidots").

-export([get_settings/0]).

get_settings() ->
    #{ubidots_cache_type =>
          list_to_atom(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__TYPE", "single")),
      ubidots_cache_host_name =>
          os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__HOST_NAME", "127.0.0.1"),
      ubidots_cache_port =>
          list_to_integer(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__PORT", "6379")),
      ubidots_cache_database =>
          list_to_integer(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__DATABASE", "1")),
      ubidots_cache_password => os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__PASSWORD", ""),
      ubidots_cache_server =>
          os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__SERVER", "localhost:6379"),
      ubidots_cache_pool_size =>
          list_to_integer(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__POOL_SIZE", "10")),
      ubidots_cache_pool_type =>
          list_to_atom(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__POOL_TYPE", "round_robin")),
      ubidots_cache_reconnect =>
          list_to_integer(os:getenv("EMQX_RETAINER__UBIDOTS_CACHE__RECONNECT", "3")),
      reactor_cache_type =>
          list_to_atom(os:getenv("EMQX_RETAINER__REACTOR_CACHE__TYPE", "single")),
      reactor_cache_host_name =>
          os:getenv("EMQX_RETAINER__REACTOR_CACHE__HOST_NAME", "127.0.0.1"),
      reactor_cache_port =>
          list_to_integer(os:getenv("EMQX_RETAINER__REACTOR_CACHE__PORT", "6379")),
      reactor_cache_database =>
          list_to_integer(os:getenv("EMQX_RETAINER__REACTOR_CACHE__DATABASE", "2")),
      reactor_cache_password => os:getenv("EMQX_RETAINER__REACTOR_CACHE__PASSWORD", ""),
      reactor_cache_server =>
          os:getenv("EMQX_RETAINER__REACTOR_CACHE__SERVER", "localhost:6379"),
      reactor_cache_pool_type =>
          list_to_atom(os:getenv("EMQX_RETAINER__REACTOR_CACHE__POOL_TYPE", "round_robin")),
      reactor_cache_pool_size =>
          list_to_integer(os:getenv("EMQX_RETAINER__REACTOR_CACHE__POOL_SIZE", "10")),
      reactor_cache_reconnect =>
          list_to_integer(os:getenv("EMQX_RETAINER__REACTOR_CACHE__RECONNECT", "3"))}.
