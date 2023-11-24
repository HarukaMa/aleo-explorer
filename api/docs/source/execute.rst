Execute API
=================

.. warning::
   All executions are *simulated*. No real execution is performed.

   APIs in this page are experimental and subject to change.

Simulate Finalize Execution
---------------------------

.. http:post:: /api/v1/simulate_execution/finalize

   Simulate finalize execution.

   .. warning::
      The simulation result might differ from the real execution result if the program relies on the on-chain state,
      including the random command.

   **Example request**:

   .. sourcecode:: http

      POST /api/v1/simulate_execution/finalize HTTP/1.1
      Host: explorer.hamp.app
      Content-Type: application/json

      {
          "inputs": [
              "aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px",
              "aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px",
              "1000000u64"
          ],
          "program_id": "credits.aleo",
          "transition_name": "transfer_public"
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      {
          "mapping_updates": [
              {
                  "key": "aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px",
                  "key_id": "3735849869114892265786124913473029534128875410754308268945151169665323186552field",
                  "mapping": "account",
                  "mapping_id": "2855157744830843716005407030207142101853521493742120919939436395872133863104field",
                  "type": "UpdateKeyValue",
                  "value": "9827614u64",
                  "value_id": "3266615198292033282950115883302413454228283640771489693608225349725511562527field"
              },
              {
                  "key": "aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px",
                  "key_id": "3735849869114892265786124913473029534128875410754308268945151169665323186552field",
                  "mapping": "account",
                  "mapping_id": "2855157744830843716005407030207142101853521493742120919939436395872133863104field",
                  "type": "UpdateKeyValue",
                  "value": "10827614u64",
                  "value_id": "4267214900870705622674540766849083498253831452142619194451382331686398522842field"
              }
          ]
      }

   :statuscode 200: no error
   :statuscode 400: wrong input parameter
   :statuscode 404: program or transition not found
   :statuscode 500: internal server error
