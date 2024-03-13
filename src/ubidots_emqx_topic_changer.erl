%%%-------------------------------------------------------------------
%%% @author ubidots
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Aug 2019 4:55 p. m.
%%%-------------------------------------------------------------------
-module(ubidots_emqx_topic_changer).

-author("ubidots").

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([remove_users_topic/2, add_users_topic/2]).

-spec remove_users_topic(emqx:topic(), emqx_types:message()) -> emqx_types:message().
remove_users_topic(Topic, Msg) ->
    NewTopic = re:replace(Topic, "/users/[^/]+", "", [{return, list}]),
    Msg#message{topic = NewTopic}.

add_users_topic(UserName, {Topic, SubOpts}) ->
    V1Start = <<"/v1.6/users/">>,
    End = <<"/">>,
    V2Start = <<"/v2.0/users/">>,
    NewTopic = re:replace(Topic, "^/v1.6/", <<V1Start/binary, UserName/binary, End/binary>>),
    {erlang:iolist_to_binary(
         re:replace(NewTopic, "^/v2.0/", <<V2Start/binary, UserName/binary, End/binary>>)),
     SubOpts}.

remove_users_topic_v1_test() ->
    Msg = emqx_message:make("/v1.6/users/token/devices/d1/v1",
                            <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
    #message{topic = Topic, payload = Payload} =
        remove_users_topic("/v1.6/users/token/devices/d1/v1", Msg),
    "/v1.6/devices/d1/v1" = Topic,
    <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">> =
        Payload.

remove_users_topic_v2_test() ->
    Msg = emqx_message:make("/v2.0/users/token/devices/d1/v1",
                            <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">>),
    #message{topic = Topic, payload = Payload} =
        remove_users_topic("/v2.0/users/token/devices/d1/v1", Msg),
    "/v2.0/devices/d1/v1" = Topic,
    <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 12}, \"created_at\": 13}">> =
        Payload.

add_users_topic_v1_test() ->
    UserName = <<"token">>,
    Topic = <<"/v1.6/devices/d1/v1">>,
    SubOpts = #{},
    {<<"/v1.6/users/token/devices/d1/v1">>, #{}} =
        add_users_topic(UserName, {Topic, SubOpts}).

add_users_topic_v2_test() ->
    UserName = <<"token">>,
    Topic = <<"/v2.0/devices/d1/v1">>,
    SubOpts = #{},
    {<<"/v2.0/users/token/devices/d1/v1">>, #{}} =
        add_users_topic(UserName, {Topic, SubOpts}).
