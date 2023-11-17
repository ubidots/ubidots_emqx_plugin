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
    {
        erlang:iolist_to_binary(
            re:replace(NewTopic, "^/v2.0/", <<V2Start/binary, UserName/binary, End/binary>>)
        ),
        SubOpts
    }.
