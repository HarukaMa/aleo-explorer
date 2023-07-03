
Mapping API
=================

.. note::
   Mapping API endpoints will refuse to return data if the node is not in sync with the network. If you still want
   the result, add ``?outdated=1`` parameter to the request. Please note the info returned will be outdated and needs to
   be handled with caution.

Get Mapping Value
-----------------

.. http:get:: /api/v1/mapping/get_value/(program_id)/(mapping_id)/(key)

   Get the mapping value, given the program ID, mapping name and key.

   Returns ``null`` if the key doesn't exist in the mapping.

   .. tip::
      Equivalent snarkOS API: ``/testnet3/program/{programID}/mapping/{mappingName}/{mappingKey}``
   .. important::
      Currently ``struct`` keys are not supported. Please either use the snarkOS API or list all mapping values.

   **Example request**:

   .. sourcecode:: http

      GET /api/v1/mapping/get_value/credits.aleo/account/aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px HTTP/1.1
      Host: explorer.hamp.app

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      "30000000u64"

   **Example error response**:

   .. sourcecode:: http

      HTTP/1.1 404 Not Found
      Content-Type: application/json

      {
          "error": "Mapping not found"
      }

   :statuscode 200: no error
   :statuscode 400: invalid mapping / key name
   :statuscode 404: program / mapping not

List Mappings in Program
------------------------

.. http:get:: /api/v1/mapping/list_program_mappings/(program_name)

   List all mappings in a program.

   .. tip::
      Equivalent snarkOS API: ``/testnet3/program/{programName}/mappings``

   **Example request**:

   .. sourcecode:: http

      GET /api/v1/mapping/list_program_mappings/credits.aleo HTTP/1.1
      Host: explorer.hamp.app

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      [
          "account"
      ]

   :statuscode 200: no error
   :statuscode 404: program not found

List All Values in Mapping
--------------------------

.. http:get:: /api/v1/mapping/list_program_mapping_values/(program_id)/(mapping_id)

      List all values in a mapping.

      **Example request**:

      .. sourcecode:: http

         GET /api/v1/mapping/list_program_mapping_values/credits.aleo/account HTTP/1.1
         Host: explorer.hamp.app

      **Example response**:

      .. sourcecode:: http

         HTTP/1.1 200 OK
         Content-Type: application/json

         [
             {
                 "index": 0,
                 "key": "aleo188l8c0ycaqslrv8k2lchx2jdfyy5fe2c4yptnny9cppmhd3rcsysjvvffc",
                 "key_id": "6717981092342298979169535710375691966949863109537399814054477596939141497911field",
                 "value": "30000000u64",
                 "value_id": "2462198501873112105920064907846691000876788024617828372841762417643460125424field"
             },
             {
                 "index": 1,
                 "key": "aleo1x5wdanv68jkx7y9s7garve3kgxu0mcexnkec8jvdc67jd6g25y9qah9hyv",
                 "key_id": "452472027313254607720670787780076688320444521393665791743503582289680559862field",
                 "value": "30000000u64",
                 "value_id": "799534246656601839550418628268894396380239773510887077116756969413724260537field"
             }
         ]

      :statuscode 200: no error
      :statuscode 404: program / mapping not found