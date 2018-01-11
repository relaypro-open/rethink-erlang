-module(rethink_scram_tests).

-compile([export_all]).

% Full flow from python driver:
%  password ''
%  client_first_message_bare n=admin,r=ZE0STAivJ4wq3BIcgQROXQAX
%  server_first_message r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp,s=fOCbe2F9Vspn+cl9tt4rwQ==,i=1
%  client_final_message_without_proof c=biws,r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp
%  b64(salt) fOCbe2F9Vspn+cl9tt4rwQ==
%  i 1
%  b64(salted_password) e5IKcXgSUuyzrfgvnIaVoY/P6qS30n7U0w8qSc08UG4=
%  b64(client_key) NSgAsVMZ5uMknWeD5zpXfYzWBxvSiV4mqL2ShehWYWI=
%  b64(stored_key) 2+wQd7lKrZTPNObUjBHobcJTB7owCoUtskF+M75OqMI=
%  auth_message n=admin,r=ZE0STAivJ4wq3BIcgQROXQAX,r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp,s=fOCbe2F9Vspn+cl9tt4rwQ==,i=1,c=biws,r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp
%  b64(client_signature) GbZcvEg83bCymjbeh+OOtiAHwvjNIHLqUTh7cYNWetc=
%  b64(client_proof) LJ5cDRslO1OWB1FdYNnZy6zRxeMfqSzM+YXp9GsAG7U=
%  b64(server_key) RaBRDOMjEiWwH45hDm8Xq2JGBgRqOS0dDHJgCS/GftI=
%  b64(server_signature) QOHEUh9Qx1d/nxmQQ1ZA2WNHS5PgmVjrJiZhrwt5a5o=
%

generate_client_proof_test() ->
    ClientFirstMessageBare = <<"n=admin,r=ZE0STAivJ4wq3BIcgQROXQAX">>,
    ServerFirstMessage = <<"r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp,s=fOCbe2F9Vspn+cl9tt4rwQ==,i=1">>,
    ClientFinalMessageWithoutProof = <<"c=biws,r=ZE0STAivJ4wq3BIcgQROXQAXnXjM66t7+1G1BLw5Ayzp">>,
    Password = <<>>,
    Salt = base64:decode(<<"fOCbe2F9Vspn+cl9tt4rwQ==">>),
    IterationCount = 1,
    {<<"LJ5cDRslO1OWB1FdYNnZy6zRxeMfqSzM+YXp9GsAG7U=">>,_,_} = rethink_scram:generate_client_proof(
      ClientFirstMessageBare, ServerFirstMessage,
      ClientFinalMessageWithoutProof, Password, Salt, IterationCount).
