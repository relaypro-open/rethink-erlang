-module(rethink_scram).

-export([method/0, get_nonce/1, get_salt/1, get_iteration_count/1,
         generate_nonce/0,
        generate_client_proof/6, r_hi/3]).

method() -> <<"SCRAM-SHA-256">>.

generate_nonce() -> <<"rOprNGfwEbeRWgbNEkqO">>.

get_nonce(X) ->
    get_field(<<"r">>, X).

get_salt(X) ->
    get_field(<<"s">>, X).

get_iteration_count(X) ->
    BinField = get_field(<<"i">>, X),
    list_to_integer(binary_to_list(BinField)).

get_field(Field, X) ->
    case re:run(X, <<Field/binary, "=(?<v>[^,]+)">>, [{capture, [v], binary}]) of
        {match, [V]} ->
            V
    end.

generate_client_proof(ClientFirstMessageBare,
                      ServerFirstMessage,
                      ClientFinalMessageWithoutProof,
                      Password, Salt, I) ->
    SaltedPassword = r_hi(r_normalize(Password), Salt, I),
    ClientKey = r_hmac(SaltedPassword, <<"Client Key">>),
    StoredKey = r_h(ClientKey),
    AuthMessage = iolist_to_binary([ClientFirstMessageBare,
                                    $,,
                                    ServerFirstMessage,
                                    $,,
                                    ClientFinalMessageWithoutProof]),
    ClientSignature = r_hmac(StoredKey, AuthMessage),
    ClientProof = r_xor(ClientKey, ClientSignature),

    ServerKey = r_hmac(SaltedPassword, "Server Key"),
    ServerSignature = r_hmac(ServerKey, AuthMessage),

    {base64:encode(ClientProof),
     base64:encode(ServerKey),
     base64:encode(ServerSignature)}.

r_normalize(X) ->
    X.

r_hi(Str, Salt, I) ->
    U0 = iolist_to_binary([Salt, r_int(1)]),
    U1 = r_hmac(Str, U0),
    [_|UList] = lists:reverse(lists:foldl(
      fun(_Iter, Acc=[UIm1|_]) ->
              UI = r_hmac(Str, UIm1),
              [UI|Acc]
      end, [U1], lists:seq(2, I))),
    lists:foldl(
      fun(U, Hi) ->
              r_xor(Hi, U)
      end, U1, UList).

r_int(1) ->
    <<0,0,0,1>>.

r_hmac(Key, Data) ->
    crypto:mac(hmac, sha256, Key, Data).

r_h(X) ->
    crypto:hash(sha256, X).

r_xor(X, Y) ->
    crypto:exor(X, Y).
