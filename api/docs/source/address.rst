
Address API
===========

.. _address-staking-info:

Address Staking Info / History
------------------------------

.. http:get:: /v2/address/staking_info/(address)

   :query height: (optional) retrieve data at a specific block height. Mutually exclusive with ``time``.
   :query time: (optional) retrieve data at a specific time. Accepts a number as unix epoch, or a string in ISO 8601 format. Mutually exclusive with ``height``.
   :>json int height: block height of the data.
   :>json int timestamp: block timestamp.
   :>json int amount: bonded amount of the address in microcredits.
   :>json int validator: validator the address is / was bonded to.

   Get the staking info of an address, now or at specific time in the past.
   Returns ``null`` if the address is not bonded.

   **Example request**:

   .. sourcecode:: http

      GET /v2/address/staking_info/aleo1sdjqhlcm9qltpu74ek0vxewt52zsdmn6swmpjn6m0tp9xf57dvpq740r8j HTTP/1.1
      Host: api.aleoscan.io

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      {
          "height": 1230000,
          "timestamp": 1712200000,
          "amount": 10000000000,
          "validator": "aleo1n6c5ugxk6tp09vkrjegcpcprssdfcf7283agcdtt8gu9qex2c5xs9c28ay"
      }

   :statuscode 200: no error
   :statuscode 400: invalid parameter or combination
   :statuscode 404: time or height is before the genesis block

