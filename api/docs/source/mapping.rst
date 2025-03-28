
Mapping API
=================

.. note::
   Mapping API endpoints will refuse to return data if the node is not in sync with the network. If you still want
   the result, add ``?outdated=1`` parameter to the request. Please note the info returned will be outdated and needs to
   be handled with caution.

.. _mapping-get-value:

Get Mapping Value
-----------------

.. http:get:: /v3/mapping/get_value/(program_id)/(mapping_id)/(key)

   :query height: (optional) retrieve data at a specific block height. Mutually exclusive with ``time``.
   :query time: (optional) retrieve data at a specific time. Accepts a number as unix epoch, or a string in ISO 8601 format. Mutually exclusive with ``height``.
   :>json int height: block height of the data.
   :>json int timestamp: block timestamp.
   :>json str value: value of the requested key in the mapping.

   Get the mapping value, given the program ID, mapping name and key. Optionally, the value at specified height or time.

   The value will be ``null`` if the key doesn't exist in the mapping.

   .. versionchanged:: 3
      Added ``height`` and ``time`` query parameters, so it's now possible to get the value at a specific block height or time.
      Note the return format has changed to include the ``height`` and ``timestamp`` fields with v3.
      Earlier versions of the API will return only the value (not changed).

   **Example request**:

   .. sourcecode:: http

      GET /v3/mapping/get_value/credits.aleo/account/aleo1rhgdu77hgyqd3xjj8ucu3jj9r2krwz6mnzyd80gncr5fxcwlh5rsvzp9px HTTP/1.1
      Host: api.aleoscan.io

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

.. http:get:: /v1/mapping/list_program_mappings/(program_name)

   List all mappings in a program.

   .. tip::
      Equivalent snarkOS API: ``/mainnet/program/{programName}/mappings``

   **Example request**:

   .. sourcecode:: http

      GET /v1/mapping/list_program_mappings/credits.aleo HTTP/1.1
      Host: api.aleoscan.io

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      [
          "committee",
          "bonded",
          "unbonding",
          "account"
      ]

   :statuscode 200: no error
   :statuscode 404: program not found

.. _mapping-list-all-values:

List All Values in Mapping
--------------------------

.. http:get:: /v2/mapping/list_program_mapping_values/(program_id)/(mapping_id)

      :query count: (optional) number of values to return (default: 50, max: 100), see remarks below
      :query cursor: (optional) cursor for pagination
      :>json int cursor: cursor for pagination
      :>json list result: list of key-value pairs

      List all values in a mapping.

      This endpoint uses cursor-based pagination. The ``cursor`` parameter is optional. If not provided, the first page
      will be returned. To get the next page, pass the ``cursor`` value returned in the ``cursor`` field of the response
      to the next request.

      **Remarks**: the ``count`` parameter doesn't guarantee the number of values returned if the mapping is
      ``credits.aleo/bonded`` or ``credits.aleo/committee``, as those mapping values are stored in and directly queried
      from Redis. Therefore, the response may contain more or less items than the provided ``count`` value. Make sure to
      iterate through the response values to get all data.

      **Warning**: the mapping values might change over multiple calls, so there is no guarantee that all results returned
      by this endpoint represents a consistent snapshot of the mapping. In the future, there will be a way to query the
      values at specific heights or times.

      **Example request**:

      .. sourcecode:: http

         GET /v2/mapping/list_program_mapping_values/credits.aleo/account?count=5 HTTP/1.1
         Host: api.aleoscan.io

      **Example response**:

      .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "cursor": 9373984,
            "result": [
                {
                    "key": "aleo1ldtk5svxnglkcd59j5uf5n4uj6mtaavg03tqtsvvhpd3mr889vyqvh8wnp",
                    "value": "50000000u64"
                },
                {
                    "key": "aleo1qu6umh5uan0gflqn68vdhg7pstnge08ndza5p6fkqfva4vsjjqrq2avdhg",
                    "value": "50000000u64"
                },
                {
                    "key": "aleo134d0lzxadftpkscc2slxaff36pwjd2fx46ayfkcn8jng658hvqxscrhgyz",
                    "value": "50000000u64"
                },
                {
                    "key": "aleo1kex2vdc7565r6c8y4078ytyvcyq9gcl0lflp2kqqz4f2u3su9u9qvyyqhw",
                    "value": "50000000u64"
                },
                {
                    "key": "aleo1fa7m2dac84nd5r86l9thsfl6sfwtayv32naav38n8yud2exuqc9su3c5py",
                    "value": "50000000u64"
                }
            ]
        }

      :statuscode 200: no error
      :statuscode 404: program / mapping not found

.. _mapping-get-key-count:

Get Mapping Key Count
---------------------

.. http:get:: /v2/mapping/get_key_count/(program_id)/(mapping_id)

   Get the number of keys in a mapping.

   **Example request**:

   .. sourcecode:: http

      GET /v2/mapping/get_key_count/credits.aleo/account HTTP/1.1
      Host: api.aleoscan.io

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      115448

   :statuscode 200: no error
   :statuscode 404: program / mapping not found