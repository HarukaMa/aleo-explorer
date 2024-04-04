
Status API
==========

Returns the status of the explorer.

.. _status:

Get Status
----------

.. http:get:: /api/v2/status

   :>json int server_time: server time in epoch.
   :>json int latest_block_height: latest block height indexed.
   :>json int latest_block_timestamp: timestamp of the latest block indexed.
   :>json int node_height: height of the snarkOS node the explorer is connected to.
   :>json int reference_height: height of the reference node configured by the explorer.

   Get the basic status of the explorer.

   **Example request**:

   .. sourcecode:: http

      GET /api/v2/status HTTP/1.1
      Host: testnet3.aleoscan.io

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Content-Type: application/json

      {
          "server_time": 1712200000,
          "latest_block_height": 1230000,
          "latest_block_timestamp": 1712200000,
          "node_height": 1234000,
          "reference_height": 1234001
      }

